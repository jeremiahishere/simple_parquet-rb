module SimpleParquet
  module Reader
    # A data page with a page header and a column chunk
    class DataPage
      attr_reader :input_io, :column_meta_data

      # @param [StringIO] input_io
      # @param [ColumnChunk] column_chunk with meta data inside 
      def initialize(input_io, column_chunk)
        @input_io = input_io
        @column_meta_data = column_chunk.meta_data
      end

      # @return [PageHeader] The metadata for the column chunk
      def page_header
        read_page_header_and_data_chunk_start unless defined? @page_header

        @page_header
      end

      # @return [Array<String>]
      def column_chunk_with_header
        [column_header] + page_data
      end

      # @return [String] The first element from the path in schema array.  Not really sure why this
      #   would ever have more than one value
      def column_header
        column_meta_data.path_in_schema.first
      end

      # @return [Array<String>] The column chunk for this page
      def page_data
        unless defined? @page_data
          @page_data = []

          go_to_data_chunk_start_pos

          num_rows.times do
            size = input_io.read(4).unpack("<l")[0]
            page_data << input_io.read(size)
          end
        end

        @page_data
      end

      # offset from the beginning of the io object for the data chunk
      def data_chunk_start_pos
        read_page_header_and_data_chunk_start unless defined? @data_chunk_start

        @data_chunk_start
      end

      # offset from the beginning of the io object for the page header
      def page_header_start_pos
        column_meta_data.data_page_offset
      end

      # Set the io to the position at the beginning of the page header
      def go_to_page_header_start_pos
        input_io.seek(page_header_start_pos, IO::SEEK_SET)
      end

      # Set the io to the position at the beginning of the data chunk
      def go_to_data_chunk_start_pos
        input_io.seek(data_chunk_start_pos, IO::SEEK_SET)
      end
      
      # Locates the start of the page header based on the column meta data, parses it, and records
      #   the current io position as the data chunk start
      #
      # The data chunk start is only known directly after the header is read
      def read_page_header_and_data_chunk_start
        go_to_page_header_start_pos 
        page_header = ::PageHeader.new
        page_header.read(proto)

        @page_header ||= page_header
        @data_chunk_start = input_io.pos
      end

      # number of rows in the column chunk
      def num_rows
        page_header.data_page_header.num_values
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
