#!/usr/bin/env ruby

# redis-cli -h dev2.sx4it.com -p 42163

require 'rubygems'
require 'redis'
require 'net/ssh'
require 'json'
require 'thread'
require 'optparse'
require 'ostruct'
require 'pp'

$logger = Logger.new(STDERR)

class Optparse4amexec

  def self.parse(args)
    options = OpenStruct.new
    options.logfile = STDERR
    options.loglevel = Logger::WARN
    options.port = 6379
    options.localhost = 'localhost'

    opts = OptionParser.new do |opts|
      opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
        options.loglevel = Logger::INFO if v
      end

      opts.on("-d", "--debug", "Run in debug mode") do
        options.loglevel = Logger::DEBUG
      end

      opts.on("-p", "--port PORTNB", OptionParser::DecimalInteger,
              "Specify port number (default 6379)") do |port|
        options.port = port
      end

      opts.on("-H", "--host HOSTIP",
              "Specify host address (default localhost)") do |host|
        options.host = host
      end
    end
    
    opts.parse!(args)
    options
  end
end



class RemoteExec

  def initialize(redishost, redisport)
    @redishost = redishost
    @redisport = redisport
    @redsub = Redis.new(:host => redishost, :port => redisport)
    @red = Redis.new(:host => redishost, :port => redisport)
    @running_cmds = {}
  end

  def execute(host, request, msg)
    # Debug: Host => {script}
    request['script'].delete!("\C-M")
  
    $logger.info "Starting execution of [#{request['script']}] on '#{host['ip']}'"
  
    # Start SSH
    Net::SSH.start(host['ip'],
                   "root",
                   :auth_methods => %w(publickey),
                   :host_key => "ssh-rsa",
                   :keys => [File.expand_path("4am-rsa")],
                   :port => host['port'].to_i,
                   :logger => $logger,
                   :verbose => :debug) do |ssh|
      puts 'debug'
      channel = ssh.open_channel do |ch|
        ch.exec(request['script']) do |ch, success|
          raise "could not execute command" unless success
  
          # "on_data" is called when the process writes something to stdout
          ch.on_data do |c, data|
            if data
              request['log'] += data
            end
            puts data
            @red.set msg, JSON.dump(request)
          end
  
          # "on_extended_data" is called when the process writes something to stderr
          ch.on_extended_data do |c, type, data|
            if data
              request['log'] += data
            end
            @red.set msg, JSON.dump(request)
          end
  
          ch.on_close do
            request['log'] += "--Connection Closed--\n"
            @red.set msg, JSON.dump(request)
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

  def stop_cmd(key)
    if @running_cmds.include? key
      @running_cmds[key].exit
      if resp = @red.get(key)
          request = JSON.parse(resp)
          request['status'] = 'killed'
          request['status_code'] = 130
          request['log'] += "--killed by user--\n"
          @red.set key, JSON.dump(request)
      end
    end
  end

  def handle_message(on)
    on.message do |channel, msg|
      $logger.info("Received #{msg} on channel #{channel}")
      if msg.split(':').last == "stop"
        stop_cmd(msg.split(':')[0..2].join(':'))
      elsif @red.exists(msg)
        puts "#{msg}"
        request = JSON.parse(@red.get(msg))
        puts request.inspect
        request['hosts'].each do |h|
          @running_cmds[msg] = Thread.new do
            begin
              request['status'] = 'running...'
              request['log'] += "--launching command--\n"
              request['log'] += "--on host #{h['ip']}:#{h['port']}--\n"
              request['log'] += "$ #{request["script"]}\n"
              @red.set msg, JSON.dump(request)
              execute(h, request, msg)
              request['status'] = "finished"
              @red.set msg, JSON.dump(request)
              @running_cmds.delete msg
            rescue => err
              $logger.error("#{$PROGRAM_NAME}: The command execution failed : #{err.inspect} #{err}")
              if resp = @red.get(msg)
                request = JSON.parse(resp)
                request['status'] = 'failed'
                request['status_code'] = 131
                request['log'] += "--internal error--\n"
                @red.set msg, JSON.dump(request)
              end
              @running_cmds.delete msg
            end
          end
        end
      else
        puts "#{msg} does not exist"
      end
    end
  end

  def run
    @redsub.subscribe('4am-command', 'new') do |on|
      handle_message(on)
    end
  end
end

begin
  options = Optparse4amexec.parse(ARGV)
  $logger = Logger.new(options.logfile)
  $logger.sev_threshold = options.loglevel
  RemoteExec.new(options.host, options.port).run
rescue OptionParser::MissingArgument => err
  $logger.fatal("#{$PROGRAM_NAME}: command line error : %s\n" % err)
  exit 1
rescue Redis::CannotConnectError => err
  $logger.fatal("#{$PROGRAM_NAME}: Redis connection error : %s\n" % err)
  exit 1
rescue Interrupt
  $logger.warn("Ctrl-C catched, shutting down...")
end
