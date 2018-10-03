module SimpleParquet
  module Writer
    # Converts a single string, number of thrift object into it's associated byte string
    class ByteStringWriter
      # @param [String, Fixnum, Array<String>, Thrift::Struct] obj
      def initialize(obj)
        @obj = obj

        @transport = Thrift::MemoryBufferTransport.new("")
        @proto = Thrift::CompactProtocol.new(@transport)
      end

      # @returns [String] The object from the constructor in byte string format
      def to_byte_string
        write_buffer(@obj)

        read_buffer
      end

      private

      # Writes the object to the string io buffer based on the type of data in the object
      def write_buffer(element)
        if element.is_a?(Array)
          array_to_byte_string(element)
        elsif element.is_a?(String)
          string_to_byte_string(element)
        elsif element.is_a?(Fixnum)
          fixnum_to_byte_string(element)
        else
          thrift_to_byte_string(element)
        end
      end

      # @return [String] The entire contents of the string buffer
      def read_buffer
        @transport.read(@transport.available)
      end

      # @param [Array<String] arr array of strings
      def array_to_byte_string(arr)
        arr.each do |element|
          length = element.length
          fixnum_to_byte_string(length)
          string_to_byte_string(element)
        end
      end

      # essentially a noop
      # @param [String] element
      def string_to_byte_string(element)
        element.force_encoding(Encoding::BINARY).each_byte do |b|
          @proto.write_byte(b)
        end
      end

      # little endian output
      # @param [Fixnum] element
      def fixnum_to_byte_string(element)
        [element].pack("l<").force_encoding(Encoding::BINARY).each_byte do |b|
          @proto.write_byte(b)
        end
      end

      # @param [Thrift::Struct] element
      def thrift_to_byte_string(element)
        element.write(@proto)
      end
    end
  end
end
