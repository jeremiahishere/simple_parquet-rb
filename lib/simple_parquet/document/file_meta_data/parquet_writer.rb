module SimpleParquet
  module Document
    module FileMetaData
      class ParquetWriter
        def initialize(file_meta_data, output_io)
          @fmd = file_meta_data
          @io = output_io
        end

        def write
          output = Support::ByteStringWriter.new(fmd).to_byte_string
          output += Support::ByteStringWriter.new(output.size).to_byte_string

          output
        end
      end
    end
  end
end
