require 'dragonfly/google_data_store/version'
require 'dragonfly'
require 'google/cloud/storage'
require 'cgi'
require 'securerandom'

Dragonfly::App.register_datastore(:google) { Dragonfly::GoogleDataStore }

module Dragonfly
  class GoogleDataStore
    attr_reader :project, :keyfile, :bucket, :domain, :root_path, :storage_headers

    def initialize(opts)
      @project = opts[:project]
      @keyfile = opts[:keyfile]
      @bucket_name = opts[:bucket]
      @domain = opts[:domain]
      @root_path = opts[:root_path]
      @storage_headers = opts[:storage_headers] || {}
    end

    def write(object, opts = {})
      ensure_bucket_exists

      headers = {'Content-Type' => object.mime_type}
      headers.merge!(opts[:headers]) if opts[:headers]
      uid = opts[:path] || Dragonfly::GoogleDataStore.generate_uid(object.name || 'file')

      bucket.create_file object.tempfile.path, uid,
        metadata: full_storage_headers(headers, object.meta),
        cache_control: headers['Cache-Control'] || nil,
        content_disposition: headers['Content-Disposition'] || nil,
        content_encoding: headers['Content-Encoding'] || nil,
        content_language: headers['Content-Language'] || nil,
        content_type: headers['Content-Type'] || nil

      uid
    end

    def read(uid)
      file = bucket.file uid
      content = file.download
      content.rewind
      [
        content.read,
        headers_to_meta(file.metadata),
      ]
    rescue
      nil
    end

    def destroy(uid)
      bucket.file(uid).delete
    rescue
      nil
    end

    def self.generate_uid(name)
      "#{Time.now.strftime '%Y/%m/%d/%H/%M/%S'}/#{SecureRandom.uuid}/#{name}"
    end

    def url_for(uid, opts={})
      return nil if @domain.nil?
      "https://#{@domain}/#{uid}"
    end

    private

    def bucket
      @bucket ||= storage.bucket(@bucket_name)
    end

    def ensure_bucket_exists
      storage.create_bucket(@bucket_name) unless bucket
    end

    def storage
      @storage ||= Google::Cloud::Storage.new(project: @project, keyfile: @keyfile)
    end

    def full_path(uid)
      File.join *[@root_path, uid].compact
    end

    def full_storage_headers(headers, meta)
      @storage_headers.merge(meta_to_headers(meta)).merge(headers)
    end

    def headers_to_meta(headers)
      json = headers['x-amz-meta-json']
      if json && !json.empty?
        unescape_meta_values(Serializer.json_decode(json))
      elsif marshal_data = headers['x-amz-meta-extra']
        Utils.stringify_keys(Serializer.marshal_b64_decode(marshal_data))
      end
    end

    def meta_to_headers(meta)
      meta = escape_meta_values(meta)
      {'x-amz-meta-json' => Serializer.json_encode(meta)}
    end

    def escape_meta_values(meta)
      meta.inject({}) {|hash, (key, value)|
        hash[key] = value.is_a?(String) ? CGI.escape(value) : value
        hash
      }
    end

    def unescape_meta_values(meta)
      meta.inject({}) {|hash, (key, value)|
        hash[key] = value.is_a?(String) ? CGI.unescape(value) : value
        hash
      }
    end
  end
end