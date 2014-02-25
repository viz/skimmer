require 'rubygems'
require 'net/http'
require 'neography'
require 'json'
require 'logger'


class Skimmer

  def configure
    @@logger = Logger.new(STDERR)
    @@logger.level = Logger::INFO
    @neo = Neography::Rest.new


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

  def add_users_to_graph

    (2000000..5000000).each do |id|
      unless (user = get_user_from_API(id)).nil? then
        #        @@logger.info("Found user: " + user )
        graph_user = add_user_to_graph(user)
        add_cards_to_graph(graph_user)
      else
        @@logger.info("No user for id: " + id.to_s )
      end
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
    ud = JSON.parse(user,{:symbolize_names => true})
    @@logger.info("User: " + ud[:id].to_s + " exists")
    if (res = Neography::Node.find("user_index", "id", ud[:id].to_s)).nil? then
      @@logger.info("User: " + ud[:id].to_s + " not in graph - adding to graph")
      user_node = Neography::Node.create( "id" => ud[:id],
                                          "username" => ud[:username],
                                          "display_name" => ud[:display_name],
                                          "profile_photo_s3id" => ud[:profile_photo_s3id],
                                          "bio_statement" => ud[:bio_statement],
                                          "location" => ud[:location],
                                          "user_type" => ud[:user_type],
                                          "mantel_card_id" => ud[:mantel_card]
                                          )
      user_node.add_to_index("user_index", "id", ud[:id])
      @neo.add_label(user_node, "User")

      user_node
    else
      res
    end

  end

  def get_users_from_graph
    @neo.get_nodes_labeled("User")
  end

  def get_cards_from_API(user_id)
    @@logger.info("Getting cards for user: " + user_id.to_s )
    uri = URI.parse("https://betatest.booodl.com/api/items/" + user_id.to_s)
    res = api_request(:get, uri, nil)

    case res
    when Net::HTTPSuccess
      res.body
    else
      nil
    end

  end

  def add_card_to_graph(card)
    @@logger.info("Adding card to graph: " + card.to_s)

    @@logger.info("Card: " + card[:id].to_s + " found")
    if (res = Neography::Node.find("card_index", "id", card[:id].to_s)).nil? then
      @@logger.info("Card: " + card[:id].to_s + " not in graph - adding to graph")
      card_node = Neography::Node.create( "id" => card[:id],
                                          "status" => card[:status],
                                          "description" => card[:description],
                                          "price" => card[:price],
                                          "price_currency" => card[:price_currency],
                                          "owner_id" => card[:owner_id],
                                          "original_owner_id" => card[:original_owner_id],
                                          "owner_comment" => card[:owner_comment],
                                          "title" => card[:title],
                                          "source_link" => card[:source_link]
                                          )
      card_node.add_to_index("card_index", "id", card[:id])
      @neo.add_label(card_node, "Card")
      card_node
    else
      res
    end
  end

  def add_cards_to_graph
    get_users_from_graph.each do |user|
      @@logger.info("Retrieved user: " + user["data"]["id"].to_s )
      cards = JSON.parse(get_cards_from_API(user["data"]["id"]),{:symbolize_names => true})[:items]
      unless cards.empty? then
        cards.each do |card|
          add_card_to_graph(card)
        end
      end
    end
  end

  def add_cards_to_graph(user)
    @@logger.info("Retrieved user: " + user.to_s )
    cards = JSON.parse(get_cards_from_API(user.id),{:symbolize_names => true})[:items]
    unless cards.empty? then
      cards.each do |card|
        graph_card = add_card_to_graph(card)
        @@logger.info("Card node: " + graph_card.to_s + " is a " + graph_card.class.to_s )
        @@logger.info("User node: " + user.to_s + " is a " + user.class.to_s )
        @neo.create_relationship("collected", user, graph_card)
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
