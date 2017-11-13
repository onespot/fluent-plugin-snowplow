require_relative '../helper'
require 'net/http'
require 'base64'
require 'yajl'
require 'fluent/test'
require 'fluent/plugin/out_snowplow'
require 'fluent/plugin/snowplow/version'
require 'snowplow-tracker'

class SnowplowOutputTestBase < Test::Unit::TestCase
  def self.port
    5176
  end

  def self.server_config
    config = {BindAddress: '127.0.0.1', Port: port}
    if ENV['VERBOSE']
      logger = WEBrick::Log.new(STDOUT, WEBrick::BasicLog::DEBUG)
      config[:Logger] = logger
      config[:AccessLog] = []
    end
    config
  end

  def self.test_http_client(**opts)
    opts = opts.merge(open_timeout: 1, read_timeout: 1)
    Net::HTTP.start('127.0.0.1', port, **opts)
  end

  # setup / teardown for servers
  def setup
    Fluent::Test.setup
    @gets = []
    @posts = []
    @requests = 0
    @dummy_server_thread = Thread.new do
      srv = WEBrick::HTTPServer.new(self.class.server_config)
      begin
        allowed_methods = %w(POST)
        srv.mount_proc('/com.snowplowanalytics.snowplow/') { |req,res|
          @requests += 1
          unless allowed_methods.include? req.request_method
            res.status = 405
            res.body = 'request method mismatch'
            next
          end

          record = {}
          if req.content_type.start_with? 'application/json'
            record[:json] = Yajl.load(req.body)
          end

          instance_variable_get("@#{req.request_method.downcase}s").push(record)

          res.status = 200
        }
        allowed_methods_i = %w(GET)
        srv.mount_proc('/i') { |req,res|
          @requests += 1
          unless allowed_methods_i.include? req.request_method
            res.status = 405
            res.body = 'request method mismatch'
            next
          end

          record = {}
          record[:query] = req.query()

          instance_variable_get("@#{req.request_method.downcase}s").push(record)

          res.status = 200
        }
        srv.mount_proc('/') { |_,res|
          res.status = 200
          res.body = 'running'
        }
        srv.start
      ensure
        srv.shutdown
      end
    end

    # to wait completion of dummy server.start()
    require 'thread'
    cv = ConditionVariable.new
    watcher = Thread.new {
      connected = false
      while not connected
        begin
          client = self.class.test_http_client
          client.request_get('/')
          connected = true
        rescue Errno::ECONNREFUSED
          sleep 0.1
        rescue StandardError => e
          p e
          sleep 0.1
        end
      end
      cv.signal
    }
    mutex = Mutex.new
    mutex.synchronize {
      cv.wait(mutex)
    }
  end

  def test_dummy_server
    client = self.class.test_http_client
    post_header = { 'Content-Type' => 'application/json' }

    assert_equal '200', client.request_get('/').code
    assert_equal '200', client.request_post('/com.snowplowanalytics.snowplow/', Yajl::Encoder.encode({'hello' => 'world'}), post_header).code

    assert_equal 1, @posts.size

    assert_equal 'world', @posts[0][:json]['hello']

    assert_equal '200', client.request_get('/i?hello=world').code

    assert_equal 1, @gets.size

    assert_equal 'world', @gets[0][:query]['hello']
  end

  def teardown
    @dummy_server_thread.kill
    @dummy_server_thread.join
  end

  def create_driver(conf, tag='test.metrics')
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::SnowplowOutput, tag).configure(conf)
  end
end

class SnowplowOutputTest < SnowplowOutputTestBase

  def test_that_it_has_a_version_number
    refute_nil ::Fluent::Snowplow::VERSION
  end

  CONFIG_GET = %[
    # Snowplow Emitter Config
    host          127.0.0.1
    port          #{port}
    buffer_size   0
    protocol      http
    method        get

    # Buffered Output Config
    buffer_type   memory
  ]

  CONFIG_POST = %[
    # Snowplow Emitter Config
    host          127.0.0.1
    port          #{port}
    buffer_size   10
    protocol      http
    method        post

    # Buffered Output Config
    buffer_type   memory
  ]

  SCHEMA = 'iglu:com.my_company/movie_poster/jsonschema/1-0-0'
  TSTAMP = Date.new(2017, 11, 1).strftime("%q")
  MESSAGE = {
      'movie_name' => 'solaris',
      'poster_country' => 'jp',
      'poster_year$dt' => Date.new(1978, 1, 1).iso8601
  }
  PAYLOAD = SnowplowTracker::SelfDescribingJson.new(SCHEMA, MESSAGE).to_json

  def test_configure
    d = create_driver CONFIG_POST
    i = d.instance
    assert_equal '127.0.0.1', i.host
    assert_equal 10, i.buffer_size
    assert_equal 'http', i.protocol
    assert_equal 'post', i.method
    assert_equal self.class.port, i.port
  end

  def test_emit_get
    aid = 'app1'

    d = create_driver CONFIG_GET
    d.emit({'application' => aid,
            'schema' => SCHEMA,
            'true_timestamp' => TSTAMP,
            'message' => Yajl::Encoder.encode(MESSAGE)})
    d.run

    assert_equal 1, @gets.size
    event = @gets[0][:query]

    assert_equal 'ue', event['e']
    assert_equal aid, event['aid']
    assert_equal TSTAMP, event['ttm']
    assert_nil event['tna']

    parsed = Yajl::Parser.parse( Base64.strict_decode64(event['ue_px']))['data']
    assert_equal SCHEMA, parsed['schema']
    assert_equal MESSAGE, parsed['data']
  end

  def test_emit_post
    aid = 'app1'

    d = create_driver CONFIG_POST
    d.emit({'application' => aid,
            'schema' => SCHEMA,
            'true_timestamp' => TSTAMP,
            'message' => Yajl::Encoder.encode(MESSAGE)})
    d.run

    assert_equal 1, @posts.size
    record = @posts[0]

    assert_equal 'iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4', record[:json]['schema']

    event = record[:json]['data'][0]
    assert_equal 'ue', event['e']
    assert_equal aid, event['aid']
    assert_equal TSTAMP, event['ttm']
    assert_nil event['tna']

    parsed = Yajl::Parser.parse( Base64.strict_decode64(event['ue_px']))['data']
    assert_equal SCHEMA, parsed['schema']
    assert_equal MESSAGE, parsed['data']
  end

  def test_accept_sdjon_input
    d = create_driver %[
                              # Snowplow Emitter Config
                              host          127.0.0.1
                              port          #{self.class.port}
                              method        post
                              is_sdjson     true
                              app_id_key    app_id
                              tstamp_key    timestamp
                              encode_base64 false

                              # Buffered Output Config
                              buffer_type   memory
                            ]

    aid = 'app2'
    d.emit({'app_id' => aid,
            'message' => Yajl::Encoder.encode(PAYLOAD),
            'timestamp' => TSTAMP
           })

    d.run

    assert_equal 1, @posts.size
    record = @posts[0]

    assert_equal 'iglu:com.snowplowanalytics.snowplow/payload_data/jsonschema/1-0-4', record[:json]['schema']

    event = record[:json]['data'][0]
    assert_equal 'ue', event['e']
    assert_equal aid, event['aid']
    assert_equal TSTAMP, event['ttm']

    assert_nil event['ue_px']

    parsed = Yajl::Parser.parse( event['ue_pr'])['data']
    assert_equal SCHEMA, parsed['schema']
    assert_equal MESSAGE, parsed['data']
  end

  def test_accept_context_inputs
    d = create_driver %[
                              # Snowplow Emitter Config
                              host          127.0.0.1
                              port          #{self.class.port}
                              method        post
                              is_sdjson     false
                              app_id_key    app_id
                              tstamp_key    timestamp
                              context_key   contexts

                              # Buffered Output Config
                              buffer_type   memory
                            ]

    aid = 'app2'
    d.emit({'app_id' => aid,
            'message' => Yajl::Encoder.encode(PAYLOAD),
            'contexts' => Yajl::Encoder.encode([PAYLOAD, PAYLOAD]),
            'timestamp' => TSTAMP
           })

    d.run

    assert_equal 1, @posts.size
    record = @posts[0]

    event = record[:json]['data'][0]

    assert_equal 2, Yajl::Parser.parse( Base64.strict_decode64(event['cx']))['data'].length

  end

  def test_accept_optional_nil_contexts

    d = create_driver %[
                              # Snowplow Emitter Config
                              host          127.0.0.1
                              port          #{self.class.port}
                              method        post
                              is_sdjson     false
                              app_id_key    app_id
                              tstamp_key    timestamp
                              context_key   contexts

                              # Buffered Output Config
                              buffer_type   memory
                            ]

    d.emit({'app_id' => 'app2',
            'message' => Yajl::Encoder.encode(PAYLOAD),
            'timestamp' => TSTAMP
           })

    d.run

    assert_equal 1, @posts.size
    record = @posts[0]

    event = record[:json]['data'][0]

    assert_nil event['cx']
  end

  def test_support_namespace
    d = create_driver %[
                              # Snowplow Emitter Config
                              host          127.0.0.1
                              port          #{self.class.port}
                              app_id_key    app_id
                              tstamp_key    timestamp
                              namespace_key namespace

                              # Buffered Output Config
                              buffer_type   memory
                            ]

    namespace = 'my_name'

    d.emit({'app_id' => 'app2',
            'message' => Yajl::Encoder.encode(PAYLOAD),
            'namespace' => namespace,
            'timestamp' => TSTAMP
           })

    d.run

    assert_equal 1, @gets.size

    event = @gets[0][:query]

    assert_equal namespace, event['tna']
  end

end
