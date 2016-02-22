# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "logstash/json"
require "stud/buffer"

# This output lets you output Metrics to InfluxDB
#
# You can learn more at http://influxdb.com[InfluxDB homepage]
class LogStash::Outputs::InfluxDB < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "influxdb"

  # The database to write
  config :db, :validate => :string, :default => "stats"

  # The hostname or IP address to reach your InfluxDB instance
  config :host, :validate => :string, :required => true

  # The port for InfluxDB
  config :port, :validate => :number, :default => 8086

  # Scheme to connect to InfluxDB
  config :scheme, :validate => :string, :default => "http"

  # The user who has access to the named database
  config :user, :validate => :string, :default => nil, :required => true

  # The password for the user who access to the named database
  config :password, :validate => :password, :default => nil, :required => true

  # Array of data points
  #
  # Supports sprintf formatting
  config :data_points, :validate => :array, :default => [], :required => true

  # This setting controls how many events will be buffered before sending a batch
  # of events. Note that these are only batched for the same series
  config :flush_size, :validate => :number, :default => 100

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 1

  public
  def register
    require "ftw" # gem ftw
    require 'cgi'
    @agent = FTW::Agent.new
    @queue = []

    @query_params = "db=#{@db}&u=#{@user}&p=#{@password.value}"
    @base_url = "#{@scheme}://#{@host}:#{@port}/write"
    @url = "#{@base_url}?#{@query_params}"

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
  end # def register

  public
  def receive(event)
    event_hash = {}
    event_hash['name'] = event.sprintf(@db)

    event_hash['points'] = []
    @data_points.each {|point| event_hash['points'] << event.sprintf(point)}

    buffer_receive(event_hash)
  end # def receive

  def flush(events, teardown = false)
    body = ''
    events.each do |ev|
      body << ev['points'].join('\n') << '\n'
    end
    post(body)
  end # def receive_bulk

  def post(body)
    begin
      @logger.debug("Post body: #{body}")
      response = @agent.post!(@url, :body => body)
    rescue EOFError
      @logger.warn("EOF while writing request or reading response header from InfluxDB",
                   :host => @host, :port => @port)
      return # abort this flush
    end

    # Consume the body for error checking
    # This will also free up the connection for reuse.
    body = ""
    begin
      response.read_body { |chunk| body += chunk }
    rescue EOFError
      @logger.warn("EOF while reading response body from InfluxDB",
                   :host => @host, :port => @port)
      return # abort this flush
    end

    if response.status != 200
      @logger.error("Error writing to InfluxDB",
                    :response => response, :response_body => body,
                    :request_body => @queue.join("\n"))
      return
    end
  end # def post

  def close
    buffer_flush(:final => true)
  end # def teardown
end # class LogStash::Outputs::InfluxDB
