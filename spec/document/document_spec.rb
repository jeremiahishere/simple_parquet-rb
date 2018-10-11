module SimpleParquet
  module Document
    RSpec.describe WriteableDocument do
      it 'converts from csv to parquet' do
        raw_csv = File.read(File.join(File.dirname(__FILE__), '..', 'fixtures', 'hotdogs.csv'))
        
        doc = WriteableDocument.from_csv(raw_csv)
        output = doc.write

        expect(output).to eq('hello world')
      end
    end
  end
end
