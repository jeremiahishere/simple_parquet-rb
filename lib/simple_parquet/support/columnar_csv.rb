require 'csv'

module SimpleParquet
  module Support
    # Converts a csv into a columnar format
    class ColumnarCsv
      attr_reader :csv
      
      def self.parse_rowlumnar_csv(raw_csv)
        self.new(raw_csv, :rowlumnar)
      end

      def self.parse_columnar_csv(raw_csv)
        self.new(raw_csv, :columnar)
      end

      # @param [String] raw_csv A raw csv string ready to be parsed
      def initialize(raw_csv, type = :rowlumnar)
        if type == :rowlumnar
          @csv = parse_rowlumnar_csv(raw_csv)
        else
          @csv = parse_columnar_csv(raw_csv)
        end
      end

      # @param [Array<Array<String>>] Each sub array includes a column header and one column of data
      # @return [CSV]
      def parse_columnar_csv(raw_csv)
        csv_string = CSV.generate(headers: true) do |csv|
          raw_csv.transpose.each do |row|
            csv << row
          end
        end

        CSV.parse(csv_string, headers: true)
      end

      # @param [String] raw_csv
      # @return [CSV]
      def parse_rowlumnar_csv(raw_csv)
        CSV.parse(raw_csv, headers: true)
      end

      # @param [String] header Which column to return
      # @return [Array<String>] All of the values in the column in the correct order
      def column(header)
        columns[header]
      end

      def headers
        csv.headers
      end

      def to_s
        csv.to_s
      end

      def rows
        csv.to_a.slice(1..-1)
      end

      def each_row
        csv.each do |row|
          yield(row)
        end
      end

      def columns
        unless defined? @columns
          @columns = {}
          headers.each do |header|
            @columns[header] = []
            each_row do |row|
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
