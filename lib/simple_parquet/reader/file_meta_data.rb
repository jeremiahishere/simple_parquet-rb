module SimpleParquet
  module Reader
    # Reads raw parquet and generated a FileMetaData thrift class
    class FileMetaData
      attr_reader :input_io, :parquet_special_string

      # @param [StringIO] input_io
      # @param [ParquetSpecialString] parquet_special_string String present at the end of the file
      #   that has an impact on the offsets for the file meta data location
      def initialize(input_io, parquet_special_string)
        @input_io = input_io
        @parquet_special_string = parquet_special_string
      end

      # @return [::FileMetaData] for the raw parquet passed into the constructor
      def fmd
        unless defined? @fmd
          input_io.seek(fmd_start_offset, IO::SEEK_END)

          @fmd = ::FileMetaData.new
          @fmd.read(proto)
        end

        @fmd
      end

      # @return [Array<::RowGroup>] Helper method
      def row_groups
        fmd.row_groups
      end

      # @return [Fixnum] Location of the file meta data byte size int32 from the end of the raw
      #   parquet (negative number where 0 is the end of the file)
      def fmd_size_offset
        -(parquet_special_string.length + 4)
      end

      # @return [Fixnum] The byte size for the file meta data object
      def fmd_size
        unless defined? @fmd_size
          input_io.seek(fmd_size_offset, IO::SEEK_END)
          # read a little endian 4 byte integer
          @fmd_size = input_io.read(4).unpack("<i")[0]
        end

        @fmd_size
      end

      # @return [Fixnum] Number of bytes from the end of the file where the encoded file meta data
      #   starts
      #
      # Note that fmd_size_offset is negative and fmd_size is positive so this all works out
      # swimmingly
      def fmd_start_offset
        fmd_size_offset - fmd_size
      end

      # Need to move this somewhere
      # Currently shared between DataPage and FileMetaData
      def proto
        unless defined? @proto
          transport = Thrift::IOStreamTransport.new(input_io, input_io)
          @proto ||= Thrift::CompactProtocol.new(transport)
        end

        @proto
      end
    end
  end
end
