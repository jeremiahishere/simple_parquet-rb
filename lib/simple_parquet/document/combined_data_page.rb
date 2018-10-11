module SimpleParquet
  module Document
    class CombinedDataPage
      attr_reader :header, :data

      def initialize(header, data)
        @header = header
        @data = data
      end

      def to_byte_string
        [header, data].collect do |part|
          Support::ByteStringWriter.new(part).to_byte_string
        end.join('')
      end

      def length
        to_byte_string.length
      end
    end
  end
end
