module SimpleParquet
  module Document
    module DataPageHeader
      class CsvReader
        attr_reader :data_page

        def initialize(data_page)
          @data_page = data_page
        end

        # todo memoize
        def data_page_length
          Support::ByteStringWriter.new(data_page).to_byte_string.size
        end

        def read
          Support::Configurator.page_header_with_defaults({
            uncompressed_page_size: data_page_length,
            compressed_page_size: data_page_length,
            data_page_header: {
              num_values: data_page_length
            }
          })
        end
      end
    end
  end
end
