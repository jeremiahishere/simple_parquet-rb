require 'simple_parquet/document/data_page/csv_reader'
require 'simple_parquet/document/data_page/parquet_writer'

module SimpleParquet
  module Document
    module DataPage
      def self.read_csv(header, values)
        CsvReader.new(header, values)
      end
    end
  end
end
