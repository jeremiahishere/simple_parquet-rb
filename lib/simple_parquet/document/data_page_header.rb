require 'simple_parquet/document/data_page_header/csv_reader'
require 'simple_parquet/document/data_page_header/parquet_writer'

module SimpleParquet
  module Document
    module DataPageHeader
      def self.read_csv(data_page)
        CsvReader.new(data_page)
      end
    end
  end
end
