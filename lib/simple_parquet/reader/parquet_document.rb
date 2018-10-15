module SimpleParquet
  module Reader
    class ParquetDocument
      attr_reader :raw_parquet_io

      # @param [StringIO] raw_parquet
      def initialize(raw_parquet)
        if raw_parquet.is_a?(StringIO)
          @raw_parquet_io = raw_parquet
        else
          @raw_parquet_io = StringIO.new(raw_parquet)
        end
      end

      def csv
        # note that this support does not support multiple row groups
        # I have no idea what will happen if multiple are here
        columns = []
        row_groups.each do |data_page_collection|
          data_page_collection.each do |data_page|
            columns << data_page.column_chunk_with_header
          end
        end

        Support::ColumnarCsv.parse_columnar_csv(columns)
      end

      def file_meta_data
        @file_meta_data ||= FileMetaData.new(raw_parquet_io, parquet_special_string)
      end

      def row_groups
        file_meta_data.row_groups.collect do |row_group|
          DataPageCollection.new(raw_parquet_io, row_group)
        end
      end

      def parquet_special_string
        ParquetSpecialString.new
      end
    end
  end
end
