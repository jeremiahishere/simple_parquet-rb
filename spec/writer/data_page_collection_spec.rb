module SimpleParquet
  module Writer
    RSpec.describe DataPageCollection do
      let(:raw_csv) do
        File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
      end

      let(:columnar_csv) { ColumnarCsv.new(raw_csv) }

      describe '.offset_for' do
        it 'returns the initial offset for the first data page' do
          dpc = DataPageCollection.new(columnar_csv, 4)

          expect(dpc.offset_for(dpc.first)).to eq(4)
        end

        it 'returns the size of all preceding data pages plus the initial offset' do
          dpc = DataPageCollection.new(columnar_csv, 4)

          expect(dpc.offset_for(dpc.last)).to eq(107)
        end
      end

      # one checks the fixture, one simulates a csv, neither is very good but they should cover all 
      # the cases
      describe '.total_size' do
        it 'returns zero if there is no data' do
          dpc = DataPageCollection.new(columnar_csv, 4)
          allow(dpc).to receive(:data_pages) { {} }

          expect(dpc.total_size).to eq(0)
        end
        it 'returns the total size of all of the data pages' do
          dpc = DataPageCollection.new(columnar_csv, 4)

          expect(dpc.total_size).to eq(186)
        end

        it 'returns the total size of all of the dat apages' do
          sample_data_page = DataPage.new('Sausage', ['hotdogs'])
          allow(sample_data_page).to receive(:length) { 5 }

          dpc = DataPageCollection.new(columnar_csv, 4)
          allow(dpc).to receive(:data_pages) do
            {
              # headers need to match the ones in columnar_csv
              'Sausage' => sample_data_page,
              'Taste' => sample_data_page
            }
          end

          expect(dpc.total_size).to eq(10)
        end
      end

      describe '.page_for_header' do
        it 'should pull the page for the specified header' do
          dpc = DataPageCollection.new(columnar_csv, 4)

          page = dpc.page_for_header('Sausage')

          expect(page.header).to eq('Sausage')
          expect(page.to_byte_string).to include('hotdog')

        end

        it 'should return nil if the header is not found' do
          dpc = DataPageCollection.new(columnar_csv, 4)

          expect(dpc.page_for_header('notacolumn')).to be_nil
        end
      end

      describe '.data_pages' do
        it 'splits the columnar csv into data page column objects' do
          dpc = DataPageCollection.new(columnar_csv, 4)

          expect(dpc.data_pages.size).to eq(columnar_csv.headers.size)
        end
      end
    end
  end
end
