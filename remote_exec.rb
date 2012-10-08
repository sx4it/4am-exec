#!/usr/bin/env ruby

# redis-cli -h dev2.sx4it.com -p 42163

require 'rubygems'
require 'redis'
require 'json'
require 'thread'
require 'optparse'
require 'pp'
require './executionworker.rb'

$logger = Logger.new(STDERR)

class Optparse4amexec

  def self.parse(args)
    options = Hash.new
    options[:logfile] = STDERR
    options[:loglevel] = Logger::WARN
    options[:port] = 6379
    options[:host] = 'localhost'

    opts = OptionParser.new do |opts|
      opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
        options[:loglevel] = Logger::INFO if v
      end

      opts.on("-d", "--debug", "Run in debug mode") do
        options[:loglevel] = Logger::DEBUG
      end

      opts.on("-p", "--port PORTNB", OptionParser::DecimalInteger,
              "Specify port number (default 6379)") do |port|
        options[:port] = port
      end

      opts.on("-H", "--host HOSTIP",
              "Specify host address (default localhost)") do |host|
        options[:host] = host
      end

      opts.on(:REQUIRED, "-i", "--id ID", "Specify unique id") do |id|
        options[:uid] = id
      end
    end
    
    begin
      opts.parse!
      mandatory = [:uid]                                         # Enforce the presence of
      missing = mandatory.select{ |param| options[param].nil? }        # the -t and -f switches
      if not missing.empty?
        error = "The following option"
        error += " is" if missing.count == 1
        error += "s are" if missing.count > 1
        error += " missing: #{missing.join(', ')}"                  #
        STDERR.puts error
        STDERR.puts opts
        return nil
      end
    rescue OptionParser::InvalidOption, OptionParser::MissingArgument
      STDERR.puts $!.to_s
      STDERR.puts opts
      return nil
    end   
    options
  end
end


class WorkerManager
  THREADNB = 10
  CHANWORKERREG = '4am-workers'
  def initialize(redishost, redisport, uid)
    @redishost = redishost
    @redisport = redisport
    @uid = uid
    @redsub = Redis.new(:host => redishost, :port => redisport)
    raise "UID is already used." unless @redsub.sadd(CHANWORKERREG, uid)
    @registered = true
    $logger.debug("Successfully registred woker as %s\n" % uid)
    @jobs = Queue.new
    @workers = []
    THREADNB.times do |threadid|
      @workers << Thread.new { start_worker(threadid) }
      $logger.debug("Created worker thread #{threadid}.")
    end
  end

  def shutdown
    @workers.each { |w| w.kill }
    @redsub.quit
    if @registered
      $logger.error("Unregistering workermanager #{@uid} failed.") unless @redsub.srem(CHANWORKERREG, @uid)
    end
  end

  def run
    @redsub.subscribe('4am-command', '4am-command-stop') do |on|
      on.message do |channel, msg|
        $logger.info("Received #{msg} on channel #{channel}")
        if channel == '4am-command'
          @jobs << msg
        elsif channel == '4am-command-stop'
          stop_cmd(msg)
        end
      end
    end
  end

  private
  def start_worker(id)
    begin
      ExecutionWorker.new(@redishost, @redisport, @uid, id, @jobs).run
    rescue => err
      $logger.error("#{$PROGRAM_NAME}: The ExecutionWorker #{id} died : #{err.inspect} #{err}")
    end
  end

  def stop_cmd(key)
     $logger.warning("#{$PROGRAM_NAME}: Command stop not implemented.")
#    @workers.each do |w|
#      if w['cmd'] == key
#        w.exit
#        if resp = @red.get(key)
#            request = JSON.parse(resp)
#            request['status'] = 'killed'
#            request['status_code'] = 130
#            request['log'] += "--killed by user--\n"
#            @red.set key, JSON.dump(request)
#        end
#      end
#    end
  end
end

def main
  options = Optparse4amexec.parse(ARGV)
  exit 1 if options == nil
  $logger = Logger.new(options[:logfile])
  $logger.sev_threshold = options[:loglevel]
  begin
    wm = WorkerManager.new(options[:host], options[:port], options[:uid])
    wm.run
  rescue Redis::CannotConnectError => err
    $logger.fatal("#{$PROGRAM_NAME}: Redis connection error : %s\n" % err)
    exit 1
  rescue Interrupt
    $logger.warn("Ctrl-C catched, shutting down...")
  ensure
    wm.shutdown if wm
  end
end

main
