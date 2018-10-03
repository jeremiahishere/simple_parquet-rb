module SimpleParquet
  module Writer
    # organizes the parts of a parquet document so that it can be written
    class ParquetDocument
      def initialize(raw_csv)
        @columnar_csv = ColumnarCsv.new(raw_csv)
      end

      # @return [String] A valid parquet file in byte string format
      def to_byte_string
        output = ""
        parts.each do |part|
          output += part.to_byte_string
        end

        output
      end

      # @return [Array<objects with to_byte_string>] The parts of the parquet file according to the
      #   current level of parquet implementation
      def parts
        [
          ByteStringWriter.new(parquet_special_string),
          data_pages,
          file_meta_data,
          ByteStringWriter.new(parquet_special_string)
        ]
      end

      # @return [DataPageCollection] With data parsed and split from the input csv
      def data_pages
        @data_pages ||= DataPageCollection.new(@columnar_csv, parquet_special_string_offset)
      end

      # @return [FileMetaData] With offsets and document locations for the data pages so they can be
      #   found when parsing
      def file_meta_data
        @file_meta_data ||= FileMetaData.new(data_pages)
      end

      # @return [String] The special delimiter found at the beginning and end of every parquet file
      def parquet_special_string
        "PAR1"
      end

      # @return [Fixnum] The size of the special delimiter, used when determing offsets from the
      #   beginning or end of the file
      def parquet_special_string_offset
        parquet_special_string.size
      end
    end
  end
end
