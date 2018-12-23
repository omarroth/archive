require "http"

class HTTP::Server
  # Instances of this class are passed to an `HTTP::Server` handler.
  class NewContext < Context
    # The `HTTP::Request` to process.
    getter request : Request

    # The `HTTP::Server::Response` to configure and write to.
    getter response : Response

    getter remote_address : Socket::IPAddress

    # :nodoc:
    def initialize(@request : Request, @response : Response, @remote_address : Socket::IPAddress)
    end
  end
end
