#!/usr/bin/ruby

# redis-cli -h dev2.sx4it.com -p 42163

require 'rubygems'
require 'redis'
require 'net/ssh'
require 'json'
require 'thread'

$sub = Redis.new(:timeout => 10, :host => 'dev2.sx4it.com', :port => 42163)
$r = Redis.new(:timeout => 10, :host => 'dev2.sx4it.com', :port => 42163)
$stop = Redis.new(:timeout => 10, :host => 'dev2.sx4it.com', :port => 42163)

$running_cmds = {}

def execute(host, request, msg)
  # Debug: Host => {script}
  puts "### Execution ###\n"
  # puts host['ip'] + " " + host['port'] + " => {\n" + request['script'] + "\n}"
  puts "### fin Execution ###\n"  
  # Start SSH
  Net::SSH.start(
                 host['ip'],
                 "root",
                 :host_key => "ssh-rsa",
                 :keys => [File.expand_path("~/.ssh/id_remote_exec_4am_rsa")],
                 :port => host['port']
   ) do |ssh|
    channel = ssh.open_channel do |ch|
      ch.exec(request['script']) do |ch, success|
        raise "could not execute command" unless success

        # "on_data" is called when the process writes something to stdout
        ch.on_data do |c, data|
          if data
            request['log'] += data
          end
          puts data
          $r.set msg, JSON.dump(request)
        end

        # "on_extended_data" is called when the process writes something to stderr
        ch.on_extended_data do |c, type, data|
          if data
            request['log'] += data
          end
          $r.set msg, JSON.dump(request)
        end

        ch.on_close {
          request['log'] += "--Connection Closed--\n"
          $r.set msg, JSON.dump(request)
        }

        # To retrieve the status code of the last command executed
        ch.on_request("exit-status") do |ch, data|
          request['status_code'] = data.read_long
        end

      end
    end
  end
end

$sub.subscribe('4am-command', 'new') do |on|
  on.message do |channel, msg|
    if msg.split(':').last == "stop"
    key = msg.split(':')[0..2].join(':')
      if $running_cmds.include? key
        $running_cmds[key].exit
        if $r.exists key
          resp = $r.get(key)
          unless resp.nil?
            request = JSON.parse(resp)
            request['status'] = 'killed'
            request['status_code'] = 130
            request['log'] += "--killed by user--\n"
            $r.set key, JSON.dump(request)
          end
        end
      end
    elsif $r.exists(msg)
      puts "#{msg}"
      request = JSON.parse($r.get(msg))
      puts request.inspect
      request['hosts'].each do |h|
        $running_cmds[msg] = Thread.new {
          request['status'] = 'running...'
          request['log'] += "--launching command--\n"
          request['log'] += "--on host #{h['ip']}:#{h['port']}--\n"
          request['log'] += "$ #{request["script"]}\n"
          $r.set msg, JSON.dump(request)
          execute(h, request, msg)
          request['status'] = "finished"
          $r.set msg, JSON.dump(request)
          $running_cmds.delete msg
        }
      end
    else
      puts "#{msg} does not exist"
    end
  end
end

