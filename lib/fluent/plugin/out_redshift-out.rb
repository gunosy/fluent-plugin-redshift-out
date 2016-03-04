module Fluent


class RedshiftOutput < BufferedOutput
  Fluent::Plugin.register_output('redshift-out', self)

  NULL_CHAR_FOR_COPY = "\\N"

  # ignore load table error. (invalid data format)
  IGNORE_REDSHIFT_ERROR_REGEXP = /^ERROR:  Load into table '[^']+' failed\./

  def initialize
    super
    require 'aws-sdk-v1'
    require 'zlib'
    require 'time'
    require 'tempfile'
    require 'pg'
    require 'csv'
    require 'multi_json'
    require 'yajl'
    ::MultiJson.use(:yajl)
  end

  config_param :record_log_tag, :string, :default => 'log'
  # s3
  config_param :aws_key_id, :string, :secret => true, :default => nil
  config_param :aws_sec_key, :string, :secret => true, :default => nil
  config_param :s3_bucket, :string
  config_param :s3_region, :string, :default => nil
  config_param :path, :string, :default => ""
  config_param :timestamp_key_format, :string, :default => 'year=%Y/month=%m/day=%d/hour=%H/%Y%m%d-%H%M'
  config_param :utc, :bool, :default => false
  # redshift
  config_param :redshift_host, :string
  config_param :redshift_port, :integer, :default => 5439
  config_param :redshift_dbname, :string
  config_param :redshift_user, :string
  config_param :redshift_password, :string, :secret => true
  config_param :redshift_tablename, :string
  config_param :redshift_schemaname, :string, :default => nil
  config_param :redshift_copy_base_options, :string , :default => "FILLRECORD ACCEPTANYDATE TRUNCATECOLUMNS"
  config_param :redshift_copy_options, :string , :default => nil
  config_param :redshift_connect_timeout, :integer, :default => 10
  # file format
  config_param :file_type, :string, :default => nil  # json, tsv, csv, msgpack
  config_param :delimiter, :string, :default => nil
  # maintenance
  config_param :maintenance_file_path, :string, :default => nil
  # for debug
  config_param :log_suffix, :string, :default => ''

  def configure(conf)
    super
    @path = "#{@path}/" unless @path.end_with?('/') # append last slash
    @path = @path[1..-1] if @path.start_with?('/')  # remove head slash
    @utc = true if conf['utc']
    @db_conf = {
      host:@redshift_host,
      port:@redshift_port,
      dbname:@redshift_dbname,
      user:@redshift_user,
      password:@redshift_password,
      connect_timeout: @redshift_connect_timeout
    }
    @delimiter = determine_delimiter(@file_type) if @delimiter.nil? or @delimiter.empty?
    $log.debug format_log("redshift file_type:#{@file_type} delimiter:'#{@delimiter}'")
    @table_name_with_schema = [@redshift_schemaname, @redshift_tablename].compact.join('.')
    @maintenance_monitor = MaintenanceMonitor.new(@maintenance_file_path)
  end

  def start
    super
    # init s3 conf
    options = {}
    if @aws_key_id && @aws_sec_key
      options[:access_key_id] = @aws_key_id
      options[:secret_access_key] = @aws_sec_key
    end
    options[:region] = @s3_region if @s3_region
    @s3 = AWS::S3.new(options)
    @bucket = @s3.buckets[@s3_bucket]
    @redshift_connection = RedshiftConnection.new(@db_conf)
    @redshift_connection.connect_start
  end

  def format(tag, time, record)
    if json?
      record.to_msgpack
    elsif msgpack?
      { @record_log_tag => record }.to_msgpack
    else
      "#{record[@record_log_tag]}\n"
    end
  end

  def write(chunk)
    $log.debug format_log("start creating gz.")
    @maintenance_monitor.check_maintenance!

    # create a gz file
    tmp = Tempfile.new("s3-")
    tmp =
      if json? || msgpack?
        create_gz_file_from_structured_data(tmp, chunk, @delimiter)
      else
        create_gz_file_from_flat_data(tmp, chunk)
      end

    # no data -> skip
    unless tmp
      $log.debug format_log("received no valid data. ")
      return false # for debug
    end

    # create a file path with time format
    s3path = create_s3path(@bucket, @path)

    # upload gz to s3
    @bucket.objects[s3path].write(Pathname.new(tmp.path),
                                  :acl => :bucket_owner_full_control)

    # close temp file
    tmp.close!

    # copy gz on s3 to redshift
    s3_uri = "s3://#{@s3_bucket}/#{s3path}"
    credentials = @s3.client.credential_provider.credentials
    sql = "copy #{@table_name_with_schema} from '#{s3_uri}'"
    sql += " CREDENTIALS 'aws_access_key_id=#{credentials[:access_key_id]};aws_secret_access_key=#{credentials[:secret_access_key]}"
    sql += ";token=#{credentials[:session_token]}" if credentials[:session_token]
    sql += "' delimiter '#{@delimiter}' GZIP ESCAPE #{@redshift_copy_base_options} #{@redshift_copy_options};"

    $log.debug format_log("start copying. s3_uri=#{s3_uri}")

    begin
      @redshift_connection.exec(sql)
      $log.info format_log("completed copying to redshift. s3_uri=#{s3_uri}")
    rescue RedshiftError => e
      if e.to_s =~ IGNORE_REDSHIFT_ERROR_REGEXP
        $log.error format_log("failed to copy data into redshift due to load error. s3_uri=#{s3_uri}"), :error=>e.to_s
        return false # for debug
      end
      raise e
    end
    true # for debug
  end

  protected

  def format_log(message)
    (@log_suffix and not @log_suffix.empty?) ? "#{message} #{@log_suffix}" : message
  end

  private

  def json?
    @file_type == 'json'
  end

  def msgpack?
    @file_type == 'msgpack'
  end

  def create_gz_file_from_flat_data(dst_file, chunk)
    gzw = nil
    begin
      gzw = Zlib::GzipWriter.new(dst_file)
      chunk.write_to(gzw)
    ensure
      gzw.close rescue nil if gzw
    end
    dst_file
  end

  def create_gz_file_from_structured_data(dst_file, chunk, delimiter)
    # fetch the table definition from redshift
    redshift_table_columns = @redshift_connection.fetch_table_columns(@redshift_tablename, @redshift_schemaname)
    if redshift_table_columns == nil
      raise "failed to fetch the redshift table definition."
    elsif redshift_table_columns.empty?
      $log.warn format_log("no table on redshift. table_name=#{@table_name_with_schema}")
      return nil
    end

    # convert json to tsv format text
    gzw = nil
    begin
      gzw = Zlib::GzipWriter.new(dst_file)
      chunk.msgpack_each do |record|
        next unless record
        begin
          hash = json? ? json_to_hash(record[@record_log_tag]) : record[@record_log_tag]
          tsv_text = hash_to_table_text(redshift_table_columns, hash, delimiter)
          gzw.write(tsv_text) if tsv_text and not tsv_text.empty?
        rescue => e
          $log.error format_log("failed to create table text from #{@file_type}. text=(#{record[@record_log_tag]})"), :error=>e.to_s
          $log.error_backtrace
        end
      end
      return nil unless gzw.pos > 0
    ensure
      gzw.close rescue nil if gzw
    end
    dst_file
  end

  def determine_delimiter(file_type)
    case file_type
    when 'json', 'msgpack', 'tsv'
      "\t"
    when "csv"
      ','
    else
      raise Fluent::ConfigError, "Invalid file_type:#{file_type}."
    end
  end

  def json_to_hash(json_text)
    return nil if json_text.to_s.empty?

    MultiJson.load(json_text)
  rescue => e
    $log.warn format_log("failed to parse json. "), :error => e.to_s
    nil
  end

  def hash_to_table_text(redshift_table_columns, hash, delimiter)
    return "" unless hash

    # extract values from hash
    val_list = redshift_table_columns.collect {|cn| hash[cn]}

    if val_list.all?{|v| v.nil?}
      $log.warn format_log("no data match for table columns on redshift. data=#{hash} table_columns=#{redshift_table_columns}")
      return ""
    end

    generate_line_with_delimiter(val_list, delimiter)
  end

  def generate_line_with_delimiter(val_list, delimiter)
    val_list.collect do |val|
      case val
      when nil
        NULL_CHAR_FOR_COPY
      when ''
        ''
      when Hash, Array
        escape_text_for_copy(MultiJson.dump(val))
      else
        escape_text_for_copy(val.to_s)
      end
    end.join(delimiter) + "\n"
  end

  def escape_text_for_copy(val)
    val.gsub(/\\|\t|\n/, {"\\" => "\\\\", "\t" => "\\\t", "\n" => "\\\n"})  # escape tab, newline and backslash
  end

  def create_s3path(bucket, path)
    timestamp_key = (@utc) ? Time.now.utc.strftime(@timestamp_key_format) : Time.now.strftime(@timestamp_key_format)
    i = 0
    begin
      suffix = "_#{'%02d' % i}"
      s3path = "#{path}#{timestamp_key}#{suffix}.gz"
      i += 1
    end while bucket.objects[s3path].exists?
    s3path
  end

  class RedshiftError < StandardError
    def initialize(msg)
      case msg
      when PG::Error
        @pg_error = msg
        super(msg.to_s)
        set_backtrace(msg.backtrace)
      else
        super
      end
    end

    attr_accessor :pg_error
  end

  class RedshiftConnection
    REDSHIFT_CONNECT_TIMEOUT = 10.0  # 10sec

    def initialize(db_conf)
      @db_conf = db_conf
      @connection = nil
      ObjectSpace.define_finalizer(self) {
        close()
      } 
    end

    attr_reader :db_conf

    def fetch_table_columns(table_name, schema_name)
      columns = nil
      exec(fetch_columns_sql(table_name, schema_name)) do |result|
        columns = result.collect{|row| row['column_name']}
      end
      columns
    end

    def exec(sql, &block)
      conn = @connection
      conn = create_redshift_connection if conn.nil?
      if block
        conn.exec(sql) {|result| block.call(result)}
      else
        conn.exec(sql)
      end
    rescue PG::Error => e
      unless @connection.nil?
        close
      end
      connect_start
      raise RedshiftError.new(e)
    ensure
      conn.close if conn && @connection.nil?
    end

    def connect_start
      @connection = create_redshift_connection
    end

    def close
      @connection.close rescue nil if @connection
      @connection = nil
    end

    private

    def create_redshift_connection
      hostaddr = IPSocket.getaddress(db_conf[:host])
      db_conf[:hostaddr] = hostaddr

      conn = PG::Connection.connect_start(db_conf)
      raise RedshiftError.new("Unable to create a new connection.") unless conn
      if conn.status == PG::CONNECTION_BAD
        raise RedshiftError.new("Connection failed: %s" % [ conn.error_message ])
      end

      socket = conn.socket_io
      poll_status = PG::PGRES_POLLING_WRITING
      until poll_status == PG::PGRES_POLLING_OK || poll_status == PG::PGRES_POLLING_FAILED
        case poll_status
        when PG::PGRES_POLLING_READING
          IO.select([socket], nil, nil, REDSHIFT_CONNECT_TIMEOUT) or
            raise RedshiftError.new("Asynchronous connection timed out!(READING)")
        when PG::PGRES_POLLING_WRITING
          IO.select(nil, [socket], nil, REDSHIFT_CONNECT_TIMEOUT) or
            raise RedshiftError.new("Asynchronous connection timed out!(WRITING)")
        end
        poll_status = conn.connect_poll
      end

      unless conn.status == PG::CONNECTION_OK
        raise RedshiftError, ("Connect failed: %s" % [conn.error_message.to_s.lines.uniq.join(" ")])
      end

      conn
    rescue => e
      conn.close rescue nil if conn
      raise RedshiftError.new(e) if e.kind_of?(PG::Error)
      raise e
    end

    def fetch_columns_sql(table_name, schema_name = nil)
      sql = "select column_name from INFORMATION_SCHEMA.COLUMNS where table_name = '#{table_name}'"
      sql << " and table_schema = '#{schema_name}'" if schema_name
      sql << " order by ordinal_position;"
      sql
    end
  end

  class MaintenanceError < StandardError
  end

  class MaintenanceMonitor
    def initialize(maintenance_file_path)
      @file_path = maintenance_file_path
    end

    def in_maintenance?
      !!(@file_path && File.exists?(@file_path))
    end

    def check_maintenance!
      if in_maintenance?
        raise MaintenanceError.new("Service is in maintenance mode - maintenance_file_path:#{@file_path}")
      end
    end
  end
end


end
