module SimpleParquet
  module Reader
    class ParquetSpecialString
      def special_string
        "PAR1"
      end

      def to_byte_string
        Support::ByteStringWriter.new(special_string).to_byte_string
      end

      def length
        to_byte_string.length
      end
    end
  end
end
