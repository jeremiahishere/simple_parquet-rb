module SimpleParquet
  module Support
    RSpec.describe ColumnarCsv do
      let(:raw_csv) do
        File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
      end

      let(:raw_columnar_csv) do
        [
          [
            'Sausage',
            'hotdog',
            'bratwurst',
            'kielbasa',
            'chorizo',
            'liverwurst',
            'andouille',
            'bologna'
          ], [
            'Taste',
            'good',
            'just ok',
            'smoky',
            'ariba',
            'irony',
            'sweet',
            'pasty'
          ]
        ]
      end

      describe '::parse_columnar_csv' do
        it 'acts like a csv' do
          csv = ColumnarCsv.parse_columnar_csv(raw_columnar_csv)

          expect(csv.headers).to eq(['Sausage', 'Taste'])
          rows = []
          csv.each_row { |row| rows << row }
          expect(rows.first.to_h).to eq({"Sausage"=>"hotdog", "Taste"=>"good"})
        end
      end

      describe '::parse_rowlumnar_csv' do
        it 'acts like a csv' do
          csv = ColumnarCsv.parse_rowlumnar_csv(raw_csv)

          expect(csv.headers).to eq(['Sausage', 'Taste'])
          rows = []
          csv.each_row { |row| rows << row }
          expect(rows.first.to_h).to eq({"Sausage"=>"hotdog", "Taste"=>"good"})
        end
      end

      describe '.rows' do
        it 'returns an array of rows with no header row' do
          csv = ColumnarCsv.new(raw_csv)

          expect(csv.rows.first).to eq(['hotdog', 'good'])
        end
      end

      describe '.each_row' do
        it 'iterates through the rows' do
          csv = ColumnarCsv.new(raw_csv)

          rows = []
          csv.each_row { |row| rows << row }

          expect(rows.first.to_h).to eq({"Sausage"=>"hotdog", "Taste"=>"good"})
        end
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
