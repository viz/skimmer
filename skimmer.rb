require 'rubygems'
require 'net/http'
require 'neography'
require 'json'
require 'logger'


class Skimmer

  def configure
    @@logger = Logger.new(STDERR)
    @@logger.level = Logger::INFO

    Neography.configure do |config|
      config.protocol           = "http://"
      config.server             = "localhost"
      config.port               = 7474
      config.directory          = ""  # prefix this path with '/'
      config.cypher_path        = "/cypher"
      config.gremlin_path       = "/ext/GremlinPlugin/graphdb/execute_script"
      config.log_file           = "neography.log"
      config.log_enabled        = false
      config.slow_log_threshold = 0    # time in ms for query logging
      config.max_threads        = 20
      config.authentication     = nil  # 'basic' or 'digest'
      config.username           = nil
      config.password           = nil
      config.parser             = MultiJsonParser
    end
  end

  def get_user_from_API(user_id)
    uri = URI.parse("https://betatest.booodl.com/api/profiles/" + user_id.to_s)
    res = api_request(:get, uri, nil)

    case res
      when Net::HTTPSuccess  
        res.body
      else
        nil    
      end
  end

  def add_user_to_graph(user)
        @@logger.info("User: " + JSON.parse(user,{:symbolize_names => true})[:id].to_s + " exists - adding to graph")


  end

  def add_users_to_graph

    (2000000..5000000).each do |id|
      unless (user = get_user_from_API(id)).nil? then
#        @@logger.info("Found user: " + user )
        add_user_to_graph(user)
      else
        @@logger.info("No user for id: " + id.to_s )        
      end
    end

    def get_cards_from_API

    end

    def add_cards_to_graph
      get_users_from_graph.each do |user|
        get_cards_from_API(user).each do |card|
          add_card_to_graph
        end
      end
    end

  end

  def api_request(method, uri, body)

    req = nil
    case method
    when :get
      req = Net::HTTP::Get.new(uri)
    when :put
      req = Net::HTTP::Put.new(uri)
    when :post
      req = Net::HTTP::Post.new(uri)
    else
      return :error
    end
    req.body = body
    req.set_content_type("application/json")

    if req.uri.scheme == "https" then
      res = Net::HTTP.start(uri.host, uri.port, :use_ssl => true) do |http|
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE
        http.ssl_version = :SSLv3
        http.request req
      end
    else
      res = Net::HTTP.start(uri.host, uri.port, :use_ssl => false) do |http|
        http.request req
      end
    end
  end


end
