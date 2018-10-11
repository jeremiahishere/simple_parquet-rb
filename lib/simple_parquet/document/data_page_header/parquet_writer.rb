module SimpleParquet
  module Document
    module DataPageHeader
      class ParquetWriter
        attr_reader :writable_obj

        def initialize(writable_obj)
          @writable_obj = writable_obj
        end

        def write
          Support::ByteStringWriter.new(writable_obj).to_byte_string
        end
      end
    end
  end
end 
