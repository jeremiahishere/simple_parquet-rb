require 'pry'

module SimpleParquet
  module Support
    module Configurator
      def self.configurate(klass, *args)
        instance = klass.new(args)

        yield(instance)

        instance
      end

      def self.set_additional_kvs(obj, additional_kvs)
        return unless additional_kvs.is_a?(Hash)

        additional_kvs.each_pair do |key, value|
          if value.is_a?(Hash)
            # do nothing because the following line is too confusing
            # obj.send(":#{key}=", send(":#{key}_with_defaults", value))
          elsif !obj.send(key)
            # puts "Setting #{key} to #{value}"
            obj.send("#{key.to_s}=".to_sym, value)
          else
            # do nothing because the field is already set
          end
        end
      end

      def self.row_group_with_defaults(additional_kvs = {})
        configurate(RowGroup) do |row_group|
          row_group.columns = additional_kvs[:columns].collect { |kvs| column_chunk_with_defaults(kvs) }

          set_additional_kvs(row_group, additional_kvs)
        end
      end

      def self.data_page_header_with_defaults(additional_kvs = {})
        configurate(DataPageHeader) do |header|
          # defaults
          header.encoding = Encoding::PLAIN
          header.definition_level_encoding = Encoding::PLAIN
          header.repetition_level_encoding = Encoding::PLAIN
          # header.statistics

          set_additional_kvs(header, additional_kvs)
        end
      end

      def self.page_header_with_defaults(additional_kvs = {})
        configurate(PageHeader) do |header|
          header.type = PageType::DATA_PAGE
          header.data_page_header = data_page_header_with_defaults(additional_kvs[:data_page_header])
          # header.crc
          # header.index_page_header
          # header.dictionary_page_header
          # header.data_page_header_v2

          set_additional_kvs(header, additional_kvs)

        end
      end

      def self.schema_element_with_defaults(additional_kvs = {})
        configurate(SchemaElement) do |element|
          element.type = Type::BYTE_ARRAY
          element.repetition_type = FieldRepetitionType::REQUIRED # probably should be required, not 100%
          element.converted_type = ConvertedType::UTF8
          element.logicalType = LogicalType::STRING(StringType.new)

          set_additional_kvs(element, additional_kvs)
        end
      end

      def self.file_meta_data_with_defaults(additional_kvs = {})
        configurate(::FileMetaData) do |fmd|
          fmd.version = 1
          fmd.schema = additional_kvs[:schema]
          fmd.row_groups = additional_kvs[:row_groups]
          
          fmd.created_by = "simple_parquet-rb"
          # fmd.key_value_metadata = [] # not sure if this is required
          # fmd.column_orders = ? # this definitely optional

          set_additional_kvs(fmd, additional_kvs)
        end
      end

      def self.column_chunk_with_defaults(additional_kvs = {})
        configurate(ColumnChunk) do |chunk|
          chunk.meta_data = column_meta_data_with_defaults(additional_kvs[:meta_data])
          # chunk.file_offset
          # chunk.file_path
          # chunk.offset_index_offset
          # chunk.offset_index_length
          # chunk.column_index_offset
          # chunk.column_index_length
          # chunk.crypto_meta_data

          set_additional_kvs(chunk, additional_kvs)
        end
      end

      def self.column_meta_data_with_defaults(additional_kvs = {})
        configurate(ColumnMetaData) do |meta_data|
          meta_data.type = Type::BYTE_ARRAY
          meta_data.encodings = [Encoding::PLAIN]
          meta_data.codec = CompressionCodec::UNCOMPRESSED
          # meta_data.key_value_metadata = 
          # meta_data.index_page_offset =
          # meta_data.dictionary_page_offset =
          # meta_data.statistics =
          # meta_data.encoding_stats =

          set_additional_kvs(meta_data, additional_kvs)
        end
      end
    end
  end
end
