module SimpleParquet
  module Document
    module DataPage
      class CsvReader
        attr_reader :header, :values

        def initialize(header, values)
          @header = header
          @values = values
        end

        def read
          values
        end
      end
    end
  end
end
