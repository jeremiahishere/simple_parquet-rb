# SimpleParquet

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/simple_parquet/rb`. To experiment with that code, run `bin/console` for an interactive prompt.

TODO: Delete this and the text above, and describe your gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'simple_parquet-rb'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install simple_parquet-rb

## Usage

TODO: Write usage instructions here

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/simple_parquet-rb.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

TODO
- move the thrift struct readers from datA_page and file_meta_data into a shared class in Support
- look into why column.meta_data.path_in_header would ever have more than one value and update
  DataPage#column_header
- clean up the Support::ColumnarCsv interface so it returns a csv string in a less annoying way
- move Reader::ParquetSpecialString to the Support namespace and use it in Writer::ParquetDocument
- look into how parquet stores integers in column chunks, the current code only has support for
  strings in Support::ByteStringWriter
- look into support for multiple row groups.  See Reader::ParquetDocument for one of the row group
  limitations in the current code
- Look into encoding including RLE
- Look into all the terrible tests comparing byte strings to each other
