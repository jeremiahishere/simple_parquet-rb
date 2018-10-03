require 'csv'

module SimpleParquet
  module Writer
    # Converts a csv into a columnar format
    class ColumnarCsv
      # @param [String] raw_csv A raw csv string ready to be parsed
      def initialize(raw_csv)
        @csv = CSV.parse(raw_csv, headers: true)
      end

      # @param [String] header Which column to return
      # @return [Array<String>] All of the values in the column in the correct order
      def column(header)
        columns[header]
      end

      def headers
        @csv.headers
      end

      def columns
        unless defined? @columns
          @columns = {}
          headers.each do |header|
            @columns[header] = []
            @csv.each do |row|
              @columns[header] << row[header]
            end
          end
        end

        @columns
      end

      def each_column
        columns.each_pair do |header, values|
          yield(header, values)
        end
      end

      def size
        columns[headers.first].size
      end

      alias :length :size
      alias :count :size
    end
  end
end
