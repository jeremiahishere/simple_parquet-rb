module SimpleParquet
  module Writer
    # Creates the file meta data footer for the parquet file
    class FileMetaData
      attr_reader :data_pages

      # @param [DataPageCollection] data_pages The single set of column chunks that this meta data
      #   respresents.  Note that if this needs to support multiple sets of chunks, a big code
      #   update is needed
      def initialize(data_pages)
        @data_pages = data_pages
      end

      # @return [String] The file meta data followed by the length of the file meta data so it can
      #  be read when parsing the file
      def to_byte_string
        output = ByteStringWriter.new(meta_data).to_byte_string
        output += ByteStringWriter.new(output.size).to_byte_string

        output
      end

      # @return [FileMetaData] Meta data for the data pages
      def meta_data
        Configurator.file_meta_data_with_defaults({
          schema: schema_meta_data,
          num_rows: data_pages.first.values.size,
          row_groups: row_groups_meta_data
        })
      end

      # @return [Array<SchemaElement>] Column header data to be attached to the file meta data
      def schema_meta_data
        # not sure if there needs to be a header schema element with the number of columns in it
        @data_pages.collect do |page|
          Configurator.schema_element_with_defaults({
            name: page.header
          })
        end
      end

      # @return [ColumnChunk] Meta data about the size and location of each column chunk
      def column_chunk(page)
        Configurator.column_chunk_with_defaults({
          file_offset: data_pages.offset_for(page),
          meta_data: {
            path_in_schema: [page.header],
            total_uncompressed_size: page.length,
            total_compressed_size: page.length,
            data_page_offset: data_pages.offset_for(page),
            num_values: page.values.length
          }
        })
      end

      # @return [Array<RowGroup>] Location of the beginning of the column chunks and the meta data
      #   for each chunk
      def row_groups_meta_data
        row_groups = []

        # there is only one row group for now
        group = RowGroup.new
        group.num_rows = data_pages.first.values.length

        group.total_byte_size = data_pages.total_size # size of the uncompressed column data
        group.file_offset = data_pages.offset_for(data_pages.first)

        columns = []
        data_pages.each do |page|
          columns << column_chunk(page)
        end
        group.columns = columns

        row_groups << group

        row_groups
      end
    end
  end
end
