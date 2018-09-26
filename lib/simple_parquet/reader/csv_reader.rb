module SimpleParquet
  module Reader
    class CsvReader
      def initialize(raw_parquet)
        @parser = Parser.new(raw_parquet)
      end

      def read(output_io)
        csv = CSV.new(output_io, headers: true)

        grid = @parser.map(&:rows)

        grid.transpose.each do |row|
          csv << row
        end

        output_io.rewind
        output_io.read
      end
    end
  end
end
