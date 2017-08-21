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
    :client_ref => args[:q_client_ref],
  ) do |client|
    puts "Listening for events..."

    client.subscribe_topic(
      :service => "events",
      :persist_ref => args[:q_persist_ref],
    ) do |sender, event, payload|
      puts "Received Event (#{event}) by sender #{sender}: #{payload[:event_type]} #{payload[:chain_id]}"
      EmsEvent.add(sender.to_i, payload) unless args[:no_persist]
    end

    # never exit - the above block is stored as a callback and is executed by another thread
    loop { sleep 10 }
  end
end

def parse_args
  args = Trollop.options do
    opt :q_hostname, "queue hostname", :type => :string
    opt :q_port,     "queue port",     :type => :integer
    opt :q_user,     "queue username", :type => :string
    opt :q_password, "queue password", :type => :string
    opt :q_persist_ref, "topic persist_ref",   :type => :string
    opt :q_client_ref,  "topic client_ref",    :type => :string
    opt :no_persist,    "dont persist events", :type => :flag
    opt :debug,      "debug", :type => :flag
  end

  args[:q_hostname]   ||= ENV["QUEUE_HOSTNAME"] || "localhost"
  args[:q_port]       ||= ENV["QUEUE_PORT"]     || "61616"
  args[:q_user]       ||= ENV["QUEUE_USER"]     || "admin"
  args[:q_password]   ||= ENV["QUEUE_PASSWORD"] || "smartvm"
  args[:q_client_ref]   ||= ENV["QUEUE_CLIENT_REF"]  || "event_handler"
  args[:q_persist_ref]  ||= ENV["QUEUE_PERSIST_REF"] || "event_handler"

  args[:q_port] = args[:q_port].to_i

  # %i(q_hostname q_port q_user q_password).each do |param|
  #   raise Trollop::CommandlineError, "--#{param} required" if args[param].nil?
  # end

  args
end

args = parse_args

main args
