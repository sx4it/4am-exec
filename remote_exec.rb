#!/usr/bin/env ruby

# redis-cli -h dev2.sx4it.com -p 42163
# Clean commands
# redis-cli keys 'cmd-host:*' | xargs redis-cli del

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
    options[:w] = 20

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

      opts.on("-i", "--id ID", "Specify unique id") do |id|
        options[:uid] = id
      end

      opts.on("-w", "--workers NB", OptionParser::DecimalInteger,
              "Specify number of workers to launch") do |w|
        raise OptionParser::InvalidOption.new "Worker should be a positive integer." if w < 0
        options[:w] = w
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
        error += " missing: #{missing.join(', ')}"
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
  WORKERNB = 10
  def initialize(redishost, redisport, uid, workernb)
    @redishost = redishost
    @redisport = redisport
    @uid = uid
    @workernb = workernb
    @workers = Hash.new
  end

  def run
    @workernb.times do |workerid|
      pid = fork
      id = "#{@uid}-#{workerid}"
      if pid
        @workers[id] = pid
        $logger.info("Created worker process #{id}.")
      else
        exit! start_worker(id)
      end
    end
    wait_for_workers
  end

  def shutdown
    @workers.each do |id, pid|
      kill "INT", pid rescue Errno::ESRCH
    end
    wait_for_workers
  end

  private
  def start_worker(id)
    begin
      ew = ExecutionWorker.new(@redishost, @redisport, id)
      $logger.info("Created ExecutionWorker #{id}.")
      ew.run
    rescue Interrupt
      $logger.warn("Interrupt catched, worker is shutting down...")
      128
    rescue => err
      $logger.fatal("#{$PROGRAM_NAME}: The ExecutionWorker #{id} died : #{err.inspect} #{err} #{caller}")
      128
    ensure
      ew.shutdown if ew
    end
  end

  def wait_for_workers
    @workers.size.times do
      pid = Process.wait
      wuid = @workers.key pid
      @workers.delete wuid
      $logger.debug("Worker process #{wuid} (#{pid}) stopped with status #{$?.exitstatus}.")
    end
  end

#  def stop_cmd(key)
#     $logger.warning("#{$PROGRAM_NAME}: Command stop not implemented.")
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
#  end
end

def main
  options = Optparse4amexec.parse(ARGV)
  exit 1 unless options
  $logger = Logger.new(options[:logfile])
  $logger.sev_threshold = options[:loglevel]
  begin
    wm = WorkerManager.new(options[:host], options[:port], options[:uid], options[:w])
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
