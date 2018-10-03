module SimpleParquet
  module Writer
    # A page for a chunk of a column including a header and encoded data
    class DataPage
      attr_reader :header, :values

      # @param [String] header The name of the csv column
      # @param [Array<String>] values The values of the csv column
      def initialize(header, values)
        @header = header
        @values = values
      end

      # @return [PageHeader] with the size and number of values set
      def page_header
        unless defined? @page_header
          @page_header = Configurator.page_header_with_defaults({
            uncompressed_page_size: page_data.length,
            compressed_page_size: page_data.length,
            data_page_header: {
              num_values: values.length
            }
          })
        end

        @page_header
      end

      # @return [String] Page header converted to a byte string
      def page_header_byte_string
        writer = ByteStringWriter.new(page_header)

        writer.to_byte_string
      end

      # @return [String] The values enocded into the proper format
      def page_data
        unless defined? @page_data
          writer = ByteStringWriter.new(values)
          @page_data = writer.to_byte_string
        end

        @page_data
      end

      # @return [String] The page header and data encoded into the proper format
      def to_byte_string
        page_header_byte_string + page_data
      end

      # @return [Fixnum] The size of the encoded header + data
      def length
        to_byte_string.length
      end
    end
  end
end
