require 'csv'
require 'pry'

module SimpleParquet
  module Writer
    class CsvWriter
      NO_IDEA_WHAT_THIS_SHOULD_BE = 42

      attr_reader :proto

      def initialize(raw_csv)
        @csv = CSV.parse(raw_csv, headers: true)

        output_io ||= "" # totally cheating here
        @transport = Thrift::MemoryBufferTransport.new(output_io)
        @proto = Thrift::CompactProtocol.new(@transport)
      end

      def current_offset
        proto.trans.available
      end

      def write
        parquet_special_string.force_encoding(Encoding::BINARY).each_byte do |b|
          proto.write_byte(b)
        end

        # write the start file descriptor
        @csv.headers.each do |header|
          # write each data chunk and associated page header
          set_data_page_offset(header, current_offset)

          page_header(header).write(proto)
          proto.write_string(column_data[header])
        end
        # write the file metadata
        file_meta_data_start = current_offset
        file_meta_data.write(proto)
        file_meta_data_end = current_offset

        # write the file meta data offset
        file_meta_data_offset = file_meta_data_end - file_meta_data_start
        puts file_meta_data_offset
        [file_meta_data_offset].pack("l<").force_encoding(Encoding::BINARY).each_byte do |b|
          proto.write_byte(b)
        end
        # write the end file descriptor
        parquet_special_string.force_encoding(Encoding::BINARY).each_byte do |b|
          proto.write_byte(b)
        end

        proto
      end

      def print
        @csv.each do |row|
          puts row.inspect
        end
      end

      def set_data_page_offset(header, offset)
        @data_page_offset ||= {}
        @data_page_offset[header] = offset
      end

      def data_page_offset(header)
        @data_page_offset ||= {}
        @data_page_offset[header]
      end

      def parquet_special_string
        "PAR1"
      end

      def column_data
        unless defined? @column_data
          @column_data = {}

          @csv.headers.each do |header|
            @column_data[header] = ""
            @csv.each do |row|
              @column_data[header] += [row[header].length].pack("l<") + row[header]
            end
          end
        end
        
        @column_data
      end

      def page_header(header)
        data_page_header = Configurator.configurate(DataPageHeader) do |dh|
          dh.num_values = num_rows
          dh.encoding = Encoding::PLAIN
          dh.definition_level_encoding = Encoding::PLAIN
          dh.repetition_level_encoding = Encoding::PLAIN
          # dh.statistics
        end

        Configurator.configurate(PageHeader) do |ph|
          ph.type = PageType::DATA_PAGE
          ph.uncompressed_page_size = column_data[header].length
          ph.compressed_page_size = column_data[header].length
          # ph.crc
          ph.data_page_header = data_page_header
          # ph.index_page_header
          # ph.dictionary_page_header
          # ph.data_page_header_v2
        end
      end

      def file_meta_data
        Configurator.configurate(FileMetaData) do |fmd|
          fmd.version = 1
          fmd.schema = schema_meta_data
          fmd.num_rows = 7
          fmd.row_groups = row_groups_meta_data
          
          # fmd.key_value_metadata = [] # not sure if this is required
          fmd.created_by = "O2O"
          # fmd.column_orders = ? # this definitely optional
        end
      end

      def num_rows
        @csv.size
      end

      # SchemaElement type:BYTE_ARRAY (6), repetition_type:OPTIONAL (1), name:"Sausage", converted_type:UTF8 (0), logicalType:<LogicalType STRING: <StringType >>
      def schema_meta_data
        schema = []
        # not sure if there needs to be a header element with the number of columns in it
        # would look sort of like this
        # header = SchemaElement.new
        # header.name = @csv.rows.first.first
        # header.num_children = num_rows
        # schema << header
        @csv.headers.each do |header|
          element = Configurator.configurate(SchemaElement) do |element|
            element.name = header
            element.type = Type::BYTE_ARRAY
            element.repetition_type = FieldRepetitionType::REQUIRED # probably should be required, not 100%
            element.converted_type = ConvertedType::UTF8
            element.logicalType = LogicalType::STRING(StringType.new)
          end

          schema << element
        end

        schema
      end

      def column_chunk(header)
        meta_data = Configurator.configurate(ColumnMetaData) do |meta_data|
          meta_data.type = Type::BYTE_ARRAY
          meta_data.encodings = [Encoding::PLAIN]
          meta_data.path_in_schema = [header]
          meta_data.codec = CompressionCodec::UNCOMPRESSED
          meta_data.num_values = num_rows
          meta_data.total_uncompressed_size = column_data[header].length
          meta_data.total_compressed_size = column_data[header].length
          # meta_data.key_value_metadata = 
          meta_data.data_page_offset = data_page_offset(header)
          # meta_data.index_page_offset =
          # meta_data.dictionary_page_offset =
          # meta_data.statistics =
          # meta_data.encoding_stats =
        end

        chunk = Configurator.configurate(ColumnChunk) do |chunk|
          # chunk.file_path

          # the docs suggest this should be the offset to the chunk meta data but I am not sure how
          # to figure that out because the meta data occurs after the offset
          chunk.file_offset = NO_IDEA_WHAT_THIS_SHOULD_BE
          chunk.meta_data = meta_data
          # chunk.offset_index_offset
          # chunk.offset_index_length
          # chunk.column_index_offset
          # chunk.column_index_length
          # chunk.crypto_meta_data
        end

        chunk
      end

      def row_groups_meta_data
        row_groups = []

        # there is only one row group for now
        group = RowGroup.new
        group.num_rows = num_rows
        group.total_byte_size = column_data.collect(&:length).inject(0, :+) # size of the uncompressed column data
        group.file_offset = data_page_offset(@csv.headers.first) # offset to the row group
        columns = []
        @csv.headers.each do |header|
          columns << column_chunk(header)
        end
        group.columns = columns

        row_groups << group

        row_groups
      end
    end
  end
end
