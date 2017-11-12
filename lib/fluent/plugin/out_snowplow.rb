require 'snowplow-tracker'

class Fluent::SnowplowOutput < Fluent::TimeSlicedOutput
  Fluent::Plugin.register_output('snowplow', self)

  desc 'The collector host. (required)'
  config_param :host, :string

  desc 'The collector port. Defaults to 80. If you wish to set events over HTTPS, you should usually set it to 443.'
  config_param :port, :integer, default: nil

  desc 'The size of the snowplow-tracker emission buffer. When using GET, buffer_size defaults to 0 because each request can only contain one event. When using POST, buffer_size defaults to 10'
  config_param :buffer_size, :integer, default: nil

  desc 'The network protocol. Defaults to http.'
  config_param :protocol, :string, default: nil

  desc 'The http method. Defaults to get.'
  config_param :method, :string, default: nil

  desc 'True if the message field contains a self describing json payload.'
  config_param :is_sdjson, :bool, default: false

  desc 'The name of the field containing the schema. Required if :is_sdjson is false.'
  config_param :schema_key, :string, default: 'schema'

  desc 'The name of the field containing the message payload.'
  config_param :message_key, :string, default: 'message'

  desc 'The name of the field containing the event custom contexts.'
  config_param :context_key, :string, default: nil

  desc 'True if the the tstamp should be treated as a true timestamp (not a device timestamp).'
  config_param :is_true_tstamp, :bool, default: true

  desc 'The name of the field containing the timestamp. Set to nil to ignore.'
  config_param :tstamp_key, :string, default: 'true_timestamp'

  desc 'The name of the field containing the Application ID'
  config_param :app_id_key, :string, default: 'application'

  desc 'The tracker namespace.'
  config_param :namespace, :string, default: nil

  desc 'Whether the payload should be base64 encoded. Defaults to true.'
  config_param :encode_base64, :bool, default: true

  def configure(conf)
    super
  end

  def start
    super

    # see https://github.com/snowplow/snowplow/wiki/Ruby-Tracker#5-emitters
    @emitter = SnowplowTracker::Emitter.new(@host, {
      buffer_size: @buffer_size,
      protocol: @protocol,
      port: @port,
      method: @method,
      on_success: ->(_) { log.debug("Flush with success on snowplow") },
      on_failure: ->(_, _) { raise "Error when flushing to snowplow" }
    }.delete_if { |_,v| v.nil? })

    @trackers = Hash.new { |hash, key| hash[key] = {} }
  end

  def stop
    @tracker.flush
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def tracker_for(application, namespace)
    @trackers[application][namespace || :default] ||= SnowplowTracker::Tracker.new(
        @emitter, nil, namespace, application, @encode_base64)
    @trackers[application][namespace || :default]
  end

  def write(chunk)
    tracker = nil

    chunk.msgpack_each do |_, _, record|
      tracker = tracker_for(
          record[@app_id_key],
          @namespace_key ? record[@namespace_key] : nil)

      # collect the self-describing json (assume already in proper format is :is_sdjson is set)
      self_describing_json = lambda { |x|
        if @is_sdjson then
          SnowplowTracker::SelfDescribingJson.new(x['schema'], x['data'])
        else
          SnowplowTracker::SelfDescribingJson.new(record[@schema_key], x)
        end
      }.call(JSON.parse(record[@message_key]))

      # Collect any contexts
      context = if @context_key.nil? then
                  nil
                else
                  JSON.parse(record[@context_key]).map { |x| SelfDescribingJson.new(x['schema'], x['data']) }
                end

      tracker.track_self_describing_event(
          self_describing_json,
          context,
          lambda {|x| @is_true_tstamp ? SnowplowTracker::TrueTimestamp.new(x) : x }.call(record[@tstamp_key]))
    end

    tracker.flush
  end
end
