require 'simple_parquet/document/file_meta_data/csv_reader'
require 'simple_parquet/document/file_meta_data/parquet_writer'

module SimpleParquet
  module Document
    module FileMetaData
      def self.read_csv(data_pages)
        CsvReader.new(data_pages).file_meta_data
      end

      # @return [FileMetaData]
      def self.read_parquet(string_io)
        ParquetReader.new(string_io).read
      end

      def self.write_csv(file_meta_data)
        CsvWriter.new(file_meta_data).write
      end

      def self.write_parquet(file_meta_data, string_io)
        ParquetWriter.new(file_meta_data, string_io).write
      end
    end
  end
end
