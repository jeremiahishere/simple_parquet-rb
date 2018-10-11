module SimpleParquet
  module Document
    class WriteableDocument
      def self.from_csv(raw_csv)
        columnar_csv = Support::ColumnarCsv.parse_rowlumnar_csv(raw_csv)
        data_pages = DataPageCollection.read_csv(columnar_csv, ParquetSpecialString.new.length)
        file_meta_data = FileMetaData.read_csv(data_pages)

        self.new(data_pages, file_meta_data)
      end

      def self.from_parquet(raw_parquet_io)
        file_meta_data = FileMetaData.read_parquet(raw_parquet_io)
        data_pages = DataPageCollection.new(file_meta_data)

        self.new(data_pages, file_meta_data)
      end

      def initialize(data_pages, file_meta_data)
        @data_pages = data_pages
        @file_meta_data = file_meta_data
      end

      def write_parquet
        parts = [
          ParquetSpecialString.new,
          DataPageCollection.write_parquet(@data_pages),
          FileMetaData.write_parquet(@file_meta_data),
          ParquetSpecialString.new
        ]

        output = ""
        parts.each do |part|
          output += part.to_byte_string
        end

        output
      end

      def write_csv
        DataPageCollection.write_csv(@data_pages)
      end
    end
  end
end
