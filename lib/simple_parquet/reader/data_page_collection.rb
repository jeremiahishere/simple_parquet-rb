module SimpleParquet
  module Reader
    # Collection of data pages and headers within a row group
    #
    # Theoretically includes entry for each column in the original document
    class DataPageCollection
      include Enumerable

      attr_reader :input_io, :row_group

      # @param [StringIO] input_io
      # @param [RowGroup] row_group
      def initialize(input_io, row_group)
        @input_io = input_io
        @row_group = row_group
      end

      # @return [Array<DataPage>] List of parsed data pages in the row group
      def data_pages
        unless defined? @data_pages
          @data_pages = []

          row_group.columns.each do |column_chunk|
            @data_pages << DataPage.new(input_io, column_chunk)
          end
        end

        @data_pages
      end

      def each
        data_pages.each do |page|
          yield(page)
        end
      end
    end
  end
end
