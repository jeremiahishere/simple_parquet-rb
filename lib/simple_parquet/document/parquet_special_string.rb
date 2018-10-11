module SimpleParquet
  module Document
    class ParquetSpecialString
      def to_s
        "PAR1"
      end

      def to_byte_string
        Support::ByteStringWriter.new(to_s).to_byte_string
      end

      def length
        to_byte_string.length
      end
    end
  end
end
