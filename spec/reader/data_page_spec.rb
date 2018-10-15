module SimpleParquet
  module Reader
    RSpec.describe DataPage do
      let(:column_chunk) do
        Support::Configurator.column_chunk_with_defaults({
          meta_data: {
            data_page_offset: 0 # the raw parquet starts at the page header
          }
        })
      end

      let(:raw_parquet) do
        # rendered data page header and column with data ['hotdogs', 'bratwurst']
        StringIO.new("\x15\x00\x150\x150,\x15\x04\x15\x00\x15\x00\x15\x00\x00\x00\a\x00\x00\x00hotdogs\t\x00\x00\x00bratwurst")
      end

      describe '.page_header' do
        it 'reads a raw page header' do
          dp = DataPage.new(raw_parquet, column_chunk)

          header = dp.page_header

          expect(header).to be_a(PageHeader)
          expect(header.compressed_page_size).to eq(24)
          expect(header.data_page_header.num_values).to eq(2)
        end
      end

      describe '.page_header_start_pos' do
        it 'returns the location of the page header so it can be parsed' do
          dp = DataPage.new(raw_parquet, column_chunk)

          expect(dp.page_header_start_pos).to eq(0)
        end
      end

      describe '.data_chunk_start_pos' do
        it 'returns the location of the data chunk so it can be parsed' do
          dp = DataPage.new(raw_parquet, column_chunk)

          expect(dp.data_chunk_start_pos).to eq(17)
        end
      end

      describe '.page_data' do
        it 'reads a raw page header and page data, then returns the page data with fewer stubs' do
          dp = DataPage.new(raw_parquet, column_chunk)

          expect(dp.page_data).to eq(['hotdogs', 'bratwurst'])
        end
      end

      describe '.num_rows' do
        it 'returns the num rows from the data page header' do
          dp = DataPage.new(raw_parquet, column_chunk)

          expect(dp.num_rows).to eq(2)
        end
      end
    end
  end
end
