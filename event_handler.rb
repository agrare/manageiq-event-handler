#!/usr/bin/env ruby

if !defined?(Rails)
  ENV["RAILS_ROOT"] ||= File.expand_path("../manageiq", __dir__)
  require File.expand_path("config/environment", ENV["RAILS_ROOT"])
end

require "trollop"
require "manageiq-messaging"

Thread.abort_on_exception = true

def main(args)
  ManageIQ::Messaging.logger = Logger.new(STDOUT) if args[:debug]

  puts "Connecting..."
  ManageIQ::Messaging::Client.open(
    :host       => args[:q_hostname],
    :port       => args[:q_port],
    :username   => args[:q_user],
    :password   => args[:q_password],
    :client_ref => "event_handler",
  ) do |client|
    puts "Listening for events..."

    client.subscribe_messages(
      :service => "events",
      :limit   => 10
    ) do |messages|
      messages.each do |msg|
        event = msg.payload
        puts "Received Event (#{msg.message}): #{events[:event_type]} #{events[:chain_id]}"
        EmsEvent.add(events[:ems_id], events)
        client.ack(msg.ack_ref)
      end
    end

    loop { sleep 5 }
  end
end

def parse_args
  args = Trollop.options do
    opt :q_hostname, "queue hostname", :type => :string
    opt :q_port,     "queue port",     :type => :integer
    opt :q_user,     "queue username", :type => :string
    opt :q_password, "queue password", :type => :string
    opt :debug,      "debug", :type => :flag
  end

  args[:q_hostname]   ||= ENV["QUEUE_HOSTNAME"] || "localhost"
  args[:q_port]       ||= ENV["QUEUE_PORT"]     || "61616"
  args[:q_user]       ||= ENV["QUEUE_USER"]     || "admin"
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"

  args[:q_port] = args[:q_port].to_i

  # %i(q_hostname q_port q_user q_password).each do |param|
  #   raise Trollop::CommandlineError, "--#{param} required" if args[param].nil?
  # end

  args
end

args = parse_args

main args
