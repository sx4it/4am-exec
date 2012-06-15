#!/usr/bin/ruby

# redis-cli -h dev2.sx4it.com -p 42163

require 'rubygems'
require 'redis'
require 'net/ssh'
require 'json'

$sub = Redis.new(:timeout => 10, :host => 'dev2.sx4it.com', :port => 42163)
$r = Redis.new(:timeout => 10, :host => 'dev2.sx4it.com', :port => 42163)

def execute(host, request)
  # Debug: Host => {script}
  puts "### Execution ###\n"
  puts host + " => {\n" + request['script'] + "\n}"
  # Start SSH
  Net::SSH.start(host, "tata", :password => "tata", :port => 22163) do |ssh|
    channel = ssh.open_channel do |ch|
      ch.exec(request['script']) do |ch, success|
        raise "could not execute command" unless success
        
        # "on_data" is called when the process writes something to stdout

        ch.on_data do |c, data|
          request['log'] += data
          puts data
        end
        
        # "on_extended_data" is called when the process writes something to stderr
        ch.on_extended_data do |c, type, data|
          puts data
        end
        
        ch.on_close {
          puts "Done !"
        }

        # To retrieve the status code of the last command executed
        ch.on_request("exit-status") do |ch, data|
          request['status_code'] = data.read_long
        end

      end
    end
    request['status'] = "finished"
  end
end

$sub.subscribe('4am-command', 'new') do |on|
  on.message do |channel, msg|    
    if $r.exists(msg)
      puts "#{msg}"
      request = JSON.parse($r.get(msg))
      puts request.inspect      
      i = 0
      while (i < request['hosts'].size)
        execute(request['hosts'][i], request)
        $r.set "cmd-host:" + "#{request['hosts_id'][i]}" + ":" + "#{request['id']}", JSON.dump(request)
        i = i + 1
      end      
    else
      puts "#{msg} does not exist"
    end
  end
end

