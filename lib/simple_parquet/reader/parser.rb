module SimpleParquet
  module Reader
    class Column
      attr_reader :rows

      def initialize
        @rows = []
      end

      def add_row(row)
        @rows << row
        @rows
      end
    end

    class Parser
      include Enumerable

      def initialize(raw_parquet)
        @io = StringIO.new(raw_parquet)
        @transport = Thrift::IOStreamTransport.new(@io, @io)
        @fmd = file_meta_data
      end

      # Yield a column, which contains each row for that column where the
      # header is the first item in the column's row list
      def each(&block)
        each_column(&block)
      end

      private

      def each_column(&block)
        @fmd.row_groups.each do |row|
          read_row_group(row, &block)
        end
      end

      def read_row_group(row, &block)
        row.columns.each do |col|
          column_meta_data = col.meta_data

          if column_meta_data.codec != CompressionCodec::UNCOMPRESSED
            raise "the column must be uncompressed"
          end

          read_data_page(row, column_meta_data, &block)
        end
      end

      def read_data_page(row, column_meta_data, &block)
        raise "only BYTE_ARRAY columns are supported" unless column_meta_data.type == Type::BYTE_ARRAY

        @io.seek(column_meta_data.data_page_offset, IO::SEEK_SET)

        path = column_meta_data.path_in_schema.join
        rows_seen = 0

        column = Column.new
        column.add_row(path) # add header

        # Keep looping until we have all the rows, for a small dataset they are
        # probably all on the first page
        while rows_seen < row.num_rows
          header = PageHeader.new
          header.read(proto)

          validate_page_header!(header)

          page_bytes = StringIO.new(@io.read(header.compressed_page_size))

          header.data_page_header.num_values.times do
            size = page_bytes.read(4).unpack("<l")[0]
            column.add_row(page_bytes.read(size))
          end

          rows_seen += header.data_page_header.num_values
        end

        yield column
      end

      # We support only data pages (no dictionaries, which I think deals with
      # nested stuff), we don't support compressed pages and the values in the
      # pages must be PLAIN
      def validate_page_header!(header)
        if header.type != PageType::DATA_PAGE
          raise "only data pages are supported"
        end

        if header.compressed_page_size != header.uncompressed_page_size
          raise "only uncompressed pages are supported"
        end

        if header.data_page_header.encoding != Encoding::PLAIN
          raise "only unencoded values are supported in pages"
        end
      end

      def file_meta_data
        # Seek to the footer, which is 8 bytes from the end. The last 8 bytes
        # is the magic PAR1
        @io.seek(-8, IO::SEEK_END)

        # Seek to the start of the FileMetaData
        fmd_size = @io.read(4).unpack("<i")[0]
        @io.seek(-(8 + fmd_size), IO::SEEK_END)

        fmd = FileMetaData.new
        fmd.read(proto)
        fmd
      end

      # safe to memo? probs
      def proto
        @proto ||= Thrift::CompactProtocol.new(@transport)
      end
    end
  end
end
