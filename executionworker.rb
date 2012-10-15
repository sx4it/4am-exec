
require 'net/ssh'
require 'redis'

# FIXME The logger should be reconfigured

class SSHWorker
  DISCOVERYTIMEOUT = 20
  attr_reader :busy
  attr_writer :kill

  class HostKeyVerifierStore
    # This class is a simple hack to get the remote host key
    attr_reader :remote_key
  
    def verify(arguments)
      # This method only store the remote public key as an instance variable
      @remote_key = arguments[:key]
      false
    end
  end

  def initialize(redishost, redisport, uid, job)
    # We create a new connection to redis
    # 
    @redishost = redishost
    @redisport = redisport
    @uid = uid
    @r = Redis.new(:host => redishost, :port => redisport)
    @job = job
    @busy = false
  end

  def run
    loop do
      cmd = @job.pop
      @busy = cmd
      begin
        if request = lock_command(cmd)
          execute request['hosts'][0], request, cmd
          request['status'] = "finished"
          @r.set cmd, JSON.dump(request)
        end
      rescue => err
        $logger.error("#{$PROGRAM_NAME}: The command execution failed : #{err.inspect} #{err} #{caller}")
        if resp = @r.get(cmd)
          request = JSON.parse resp
          request['status'] = 'failed'
          request['status_code'] = 131
          request['log'] += "--internal error--\n"
          @r.set cmd, JSON.dump(request)
        end
      end
      @busy = false
    end
  end

  private
  def lock_command(key)
    # This method attempt to lock the command for execution
    $logger.debug("Worker thread #{@uid} attempting lock for command '#{key}'.")
    @r.watch key
    request = JSON.parse(@r.get(key))
    if request['locked']
      $logger.debug("Worker thread #{@uid} : command '#{key}' is already locked.")
      @r.unwatch
      return nil
    end
    request['locked'] = @uid
    request['status'] = 'processing'
    request['processing_start'] = Time.now
    @r.multi
    @r.set key, JSON.dump(request)
    if @r.exec
      $logger.info("Worker thread #{@uid} locked command '#{key}'.")
      request
    else
      $logger.debug("Worker thread #{@uid} : command '#{key}' has been modified, aborting lock.")
      nil
    end
  end

  def get_host_key(ip, port=22, timeout=DISCOVERYTIMEOUT)
    options[:port] = port
    options[:timeout] = timeout
    options[:paranoid] = HostKeyVerifierStore.new
    #FIXME logger
    #FIXME the exception could be abnormal if the connection failed
    Net::SSH::Transport::Session.new(ip, options) rescue Net::SSH::Exception
    options[:paranoid].remote_key
  end

  def execute(host, request, msg)
    # Debug: Host => {script}
    request['script'].delete!("\C-M")
  
    $logger.info "Starting execution of [#{request['script']}] on '#{host['ip']}:#{host['port']}'"
  
    # Start SSH
    Net::SSH.start(host['ip'],
                   "root",
                   :auth_methods => %w(publickey),
                   :host_key => "ssh-rsa",
                   :keys => [File.expand_path("4am-rsa")],
                   :port => host['port'].to_i,
                   :logger => $logger,
                   :verbose => :debug) do |ssh|
      ssh.open_channel do |ch|
        ch.exec(request['script']) do |ch, success|
          raise "could not execute command" unless success
          request['status'] = 'running'
          request['log'] += "--launching command "
          request['log'] += "on host #{host['ip']}:#{host['port']}--\n"
          request['log'] += "$ #{request["script"]}\n"
          @r.set msg, JSON.dump(request)
  
          # "on_data" is called when the process writes something to stdout
          ch.on_data do |c, data|
            request['log'] += data if data
            @r.set msg, JSON.dump(request)
          end
  
          # "on_extended_data" is called when the process writes something to stderr
          ch.on_extended_data do |c, type, data|
            request['log'] += data if data
            @r.set msg, JSON.dump(request)
          end
  
          ch.on_close do
            request['log'] += "--Connection Closed--\n"
            @r.set msg, JSON.dump(request)
          end
  
          # To retrieve the status code of the last command executed
          ch.on_request("exit-status") do |ch, data|
            request['status_code'] = data.read_long
          end
        end # End ch.exec
      end # End channel
      ssh.loop
    end # End ssh connection
    $logger.info "End execution of [#{request['script']}]"
  end
end

class ExecutionWorker
  COMMANDKILLED = 1
  WORKERKILLED = 2
  SETWORKERREG = '4am-workers'
  def initialize(redishost, redisport, uid)
    # We create a new connection to redis and add the worker uid to a specific set
    # to signal our presence
    # The steps of the initialization are as follow :
    #   1. We create a new connection to redis
    #   2. We register the worker uid to a specific set to signal our presence.
    #     The worker uid is then also used to lock the commands.
    @redishost = redishost
    @redisport = redisport
    @uid = uid
    @r = Redis.new(:host => redishost, :port => redisport)
    raise "UID is already used." unless @r.sadd(SETWORKERREG, uid)
    @registered = true
    $logger.info("Successfully registred woker as %s\n" % uid)
  end

  def run
    # As the subscribe method implemented by the redis gem blocks the whole thread,
    # we create a dedicated thread to execute the commands.
    @job = Queue.new
    @sshworker = SSHWorker.new(@redishost, @redisport, @uid, @job)
    @t = Thread.new do
      begin
        @sshworker.run
      rescue => err
        $logger.fatal("#{$PROGRAM_NAME}: The SSHWorker thread #{@uid} died : #{err.inspect} #{err} #{caller}")
        return 128
      end
    end
    @r.subscribe('4am-command', '4am-command-stop', '4am-workers-stop') do |on|
      on.message do |channel, msg|
        $logger.info("Received #{msg} on channel #{channel}")
        if channel == '4am-command'
          @job << msg unless @sshworker.busy
        elsif channel == '4am-command-stop' and msg == @sshworker.busy
          @sshworker.kill = COMMANDKILLED
        elsif channel == '4am-workers-stop' and msg == @uid
          @sshworker.kill = WORKERKILLED
          @r.unsubscribe('4am-command', '4am-command-stop', '4am-workers')
        end
      end
    end
  end

  def shutdown
    # Clean shutdown
    # FIXME We need to properly shutdown the ssh thread
    if @registered
      $logger.error("Unregistering workermanager #{@uid} failed.") unless @r.srem(SETWORKERREG, @uid)
      @registered = false
    end
    @r.quit if @r
  end

end

