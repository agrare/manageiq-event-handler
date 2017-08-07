require "manageiq-messaging"

ManageIQ::Messaging.logger = Logger.new(STDOUT)
ManageIQ::Messaging::Client.open(
  :host => "localhost",
  :port => 61616,
  :user => "admin",
  :password => "smartvm",
  :client_ref => "event_handler",
) do |client|
  puts "Listening for events..."

  client.subscribe_messages(
    :service => "events",
    :limit   => 10
  ) do |messages|
    messages.each do |msg|
      event = msg.payload
      puts "Received event #{event[:event_type]} #{event[:chain_id]}"
      EmsEvent.add(event[:ems_id], event)
      client.ack(msg.ack_ref)
    end
  end

  loop { sleep 5 }
end
