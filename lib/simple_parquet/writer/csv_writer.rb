require 'csv'

module SimpleParquet
  module Writer
    class CsvWriter
      NO_IDEA_WHAT_THIS_SHOULD_BE = 42

      def initialize(raw_csv)
        @csv = CSV.parse(raw_csv, headers: true)
      end

      def write(output_io = nil)
        output_io ||= "" # totally cheating here
        transport = Thrift::MemoryBufferTransport.new(output_io)
        proto = Thrift::CompactProtocol.new(transport)

        # write the start file descriptor
        # write each data chunk and associated page header
        # write the file metadata
        file_meta_data.write(proto)
        # write the file meta data offset
        # write the end file descriptor

        proto
      end

      def to_s
        @csv.each do |row|
          puts row.inspect
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

      def row_groups_meta_data
        row_groups = []

        group = RowGroup.new
        group.num_rows = num_rows
        group.total_byte_size = NO_IDEA_WHAT_THIS_SHOULD_BE # size of the uncompressed column data
        group.file_offset = NO_IDEA_WHAT_THIS_SHOULD_BE # offset to the row group
        columns = []
        @csv.headers.each do |header|
          meta_data = Configurator.configurate(ColumnMetaData) do |meta_data|
            meta_data.type = Type::BYTE_ARRAY
            meta_data.encodings = [Encoding::PLAIN]
            meta_data.path_in_schema = [header]
            meta_data.codec = CompressionCodec::UNCOMPRESSED
            meta_data.num_values = num_rows
            meta_data.total_uncompressed_size = NO_IDEA_WHAT_THIS_SHOULD_BE
            meta_data.total_compressed_size = NO_IDEA_WHAT_THIS_SHOULD_BE
            # meta_data.key_value_metadata = 
            meta_data.data_page_offset = NO_IDEA_WHAT_THIS_SHOULD_BE
            # meta_data.index_page_offset =
            # meta_data.dictionary_page_offset =
            # meta_data.statistics =
            # meta_data.encoding_stats =
          end

          chunk = Configurator.configurate(ColumnChunk) do |chunk|
            # chunk.file_path
            chunk.file_offset = NO_IDEA_WHAT_THIS_SHOULD_BE
            chunk.meta_data = meta_data
            # chunk.offset_index_offset
            # chunk.offset_index_length
            # chunk.column_index_offset
            # chunk.column_index_length
            # chunk.crypto_meta_data
          end
          
          columns << chunk
        end
        group.columns = columns

        row_groups << group

        row_groups
      end
    end
  end
end
