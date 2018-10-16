module SimpleParquet
  module Writer
    RSpec.describe FileMetaData do
      let(:data_pages) do
        raw_csv = File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
        csv = Support::ColumnarCsv.new(raw_csv)
        initial_offset = "PAR1".length

        DataPageCollection.new(csv, initial_offset)
      end

      describe '.to_byte_string' do
        it 'returns the byte string version of the file meta data followed by the size' do
          fmd = FileMetaData.new(data_pages)

          actual_output = fmd.to_byte_string
          expected_output = "\x15\x02\x19,\x15\f%\x00\x18\aSausage%\x00L\x1C\x00\x00\x00\x15\f%\x00\x18\x05Taste%\x00L\x1C\x00\x00\x00\x16\x0E\x19\x1C\x19,&\b\x1C\x15\f\x19\x15\x00\x19\x18\aSausage\x15\x00\x16\x0E\x16\xCE\x01\x16\xCE\x01&\b\x00\x00&\xD6\x01\x1C\x15\f\x19\x15\x00\x19\x18\x05Taste\x15\x00\x16\x0E\x16\xA6\x01\x16\xA6\x01&\xD6\x01\x00\x00\x16\xF4\x02\x16\x0E&\b\x00(\x11simple_parquet-rb\x00\x8C\x00\x00\x00".force_encoding(Encoding::BINARY)
          expect(actual_output).to eq(expected_output)
        end
      end

      describe '.meta_data' do
        it 'includes top level fields' do
          fmd = FileMetaData.new(data_pages)

          meta_data = fmd.meta_data

          expect(meta_data.num_rows).to eq(7)
        end

        it 'includes schema meta data' do
          fmd = FileMetaData.new(data_pages)

          meta_data = fmd.meta_data
          schema = meta_data.schema

          expect(schema).to be_kind_of(Array)

          schema.each do |element|
            expect(data_pages.headers).to include(element.name)
          end
        end

        it 'includes a row group' do
          fmd = FileMetaData.new(data_pages)

          meta_data = fmd.meta_data
          row_group = meta_data.row_groups.first

          expect(row_group.total_byte_size).to eq(186)
          # the first row group's offset should be the initial offset
          expect(row_group.file_offset).to eq(4)
        end

        it 'includes row groups with a column chunk for each column' do
          fmd = FileMetaData.new(data_pages)

          meta_data = fmd.meta_data
          row_group = meta_data.row_groups.first

          column_chunks = row_group.columns

          expect(column_chunks.size).to eq(2)

          sausage_chunk = column_chunks.select { |c| c.meta_data.path_in_schema.first == 'Sausage' }.first
          expect(sausage_chunk.file_offset).to eq(4)
          expect(sausage_chunk.meta_data.total_uncompressed_size).to eq(103)
          expect(sausage_chunk.meta_data.total_compressed_size).to eq(103)
          # the first chunk's offset should be the initial offset
          expect(sausage_chunk.meta_data.data_page_offset).to eq(4)
          expect(sausage_chunk.meta_data.num_values).to eq(7)

          taste_chunk = column_chunks.select { |c| c.meta_data.path_in_schema.first == 'Taste' }.first
          expect(taste_chunk.file_offset).to eq(107)
          expect(taste_chunk.meta_data.total_uncompressed_size).to eq(83)
          expect(taste_chunk.meta_data.total_compressed_size).to eq(83)
          # should be size of PAR1 + size of sausage chunk
          expect(taste_chunk.meta_data.data_page_offset).to eq(107)
          expect(taste_chunk.meta_data.num_values).to eq(7)
        end
      end
    end
  end
end
