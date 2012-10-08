
require 'net/ssh'
require 'redis'

class ExecutionWorker

  def initialize(redishost, redisport, uid, workerid, jobs)
    # We create a new connection to redis
    # 
    @redishost = redishost
    @redisport = redisport
    @uid = uid
    @workerid = workerid
    @r = Redis.new(:host => redishost, :port => redisport)
    @jobs = jobs
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
    end # End ssh connection
    $logger.info "End execution of [#{request['script']}]"
  end

  def lock_cmd(key)
    # This method attempt to lock the command for execution
    Thread.current['cmd'] = key
    @r.watch key
    request = JSON.parse(@r.get(key))
    if request['locked']
      Thread.current['cmd'] = ''
      @r.unwatch
      return nil
    end
    request['locked'] = @uid
    request['status'] = 'processing'
    request['processing_start'] = Time.now
    @r.multi
    @r.set key, JSON.dump(request)
    if @r.exec
      $logger.debug("Worker thread #{@workerid} locked command with key #{key} for execution.")
      request
    else
      Thread.current['cmd'] = ''
      nil
    end
  end

  def run
    while true
      cmd = @jobs.pop
      begin
        request = lock_cmd(cmd)
        execute(request['hosts'][0], request, cmd)
        request['status'] = "finished"
        @r.set cmd, JSON.dump(request)
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
    end
  end

end
