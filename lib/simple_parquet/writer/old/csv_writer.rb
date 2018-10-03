require 'csv'

# This was the original csv -> parquet converter.  It has been superceded by the contents of the
# SimpleParquet::Writer namespace.  Delete this at some point.

module SimpleParquet
  module Writer
    class CsvWriter
      attr_reader :proto

      def initialize(raw_csv)
        @csv = CSV.parse(raw_csv, headers: true)

        output_io ||= "" # totally cheating here
      end

      def current_offset
        proto.trans.available
      end

      # IMPORTANT NOTE: the problem with this solution is that the page offsets cannot be calculated
      # until the parts can figure out how big they are
      def write
        parts = [ ]
        parts << parquet_special_string

        # write the start file descriptor
        @csv.headers.each do |header|
          # write each data chunk and associated page header
          set_data_page_offset(header, current_offset)

          parts << page_header(header)
          parts << column_data[header]
        end
        # write the file metadata
        file_meta_data_start = current_offset
        parts << file_meta_data
        file_meta_data_end = current_offset

        # write the file meta data offset
        file_meta_data_offset = file_meta_data_end - file_meta_data_start
        parts << file_meta_data_offset
        # write the end file descriptor
        parts << parquet_special_string

        writer = ParquetWriter.new(parts)
        writer.write
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
        Configurator.page_header_with_defaults({
          uncompressed_page_size: column_data[header].length,
          compressed_page_size: column_data[header].length,
          data_page_header: {
            num_values: num_rows
          }
        })
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
          schema << Configurator.schema_element_with_defaults({
            name: header
          })
        end

        schema
      end

      def column_chunk(header)
        Configurator.column_chunk_with_defaults({
          meta_data: {
            path_in_schema: [header],
            total_uncompressed_size: column_data[header].length,
            total_compressed_size: column_data[header].length,
            data_page_offset: data_page_offset(header),
            num_values: num_rows
          }
        })
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
