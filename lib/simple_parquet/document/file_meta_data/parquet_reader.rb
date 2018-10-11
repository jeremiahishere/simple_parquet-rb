module SimpleParquet
  module Document
    module FileMetaData
      class ParquetReader
        def initialize(string_io)
          @io = string_io
        end

        def read
          if @io.read(4) != "PAR1"
            raise "Invalid parquet file"
          end

          @io.seek(fmd_size, IO::SEEK_END)

          fmd = FileMetaData.new
          fmd.read(proto)
          fmd
        end

        def fmd_start
          # Seek to the footer, which is 8 bytes from the end. The last 8 bytes
          # is the magic PAR1
          @io.seek(-8, IO::SEEK_END)

          # Seek to the start of the FileMetaData
          fmd_size = @io.read(4).unpack("<i")[0]

          -(8 + fmd_size)
        end
      end
    end
  end
end
