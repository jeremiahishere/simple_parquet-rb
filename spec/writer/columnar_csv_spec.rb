module SimpleParquet
  module Writer
    RSpec.describe ColumnarCsv do
      let(:raw_csv) do
        File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
      end

      describe '.column' do
        it 'returns the column data' do
          csv = ColumnarCsv.new(raw_csv)

          expect(csv.column('Sausage').first).to eq('hotdog')
        end

        it 'returns an array with the same length as the csv' do
          csv = ColumnarCsv.new(raw_csv)

          expect(csv.column('Sausage').count).to eq(csv.count)
        end
      end

      describe '.columns' do
        it 'returns all the data in columnar format' do
          csv = ColumnarCsv.new(raw_csv)
          columns = csv.columns

          expect(columns.size).to eq(2)
          expect(columns['Sausage']).to eq([
            "hotdog",
            "bratwurst",
            "kielbasa",
            "chorizo",
            "liverwurst",
            "andouille",
            "bologna"
          ])
          expect(columns['Taste']).to eq([
            "good",
            "just ok",
            "smoky",
            "ariba",
            "irony",
            "sweet",
            "pasty"
          ])
        end
      end

      describe 'each_column' do
        it 'returns the columns one by one' do
          csv = ColumnarCsv.new(raw_csv)
          csv.each_column do |header, value|
            expect(csv.headers).to include(header)
            expect(value).to eq(csv.column(header))
          end
        end
      end

      describe 'size' do
        it 'returns the number of rows in the csv' do
          csv = ColumnarCsv.new(raw_csv)

          expect(csv.size).to eq(7)
        end
      end
    end
  end
end
