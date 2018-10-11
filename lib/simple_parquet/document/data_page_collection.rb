module SimpleParquet
  module Document
    class DataPageCollection
      def self.read_csv(columnar_csv, initial_offset)
        output = []
        columnar_csv.each_column do |header, values|
          data_page = DataPage::CsvReader.new(header, values).read
          page_header = DataPageHeader::CsvReader.new(data_page).read
          output << CombinedDataPage.new(page_header, data_page)
        end

        self.new(output)
      end

      def self.read_parquet()
      end

      def self.write_csv(data_page_collection)
      end

      def self.write_parquet(data_page_collection, string_io)
        ParquetWriter.new(data_page_collection, string_io).write
      end

      include Enumerable

      attr_reader :combined_pages

      def initialize(combined_pages)
        @combined_pages = combined_pages
      end

      def total_size
        collect { |p| p.data.length }.inject(0, :+)
      end

      def each
        combined_pages.each do |c|
          yield(c)
        end
      end
    end
  end
end
