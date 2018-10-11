lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "simple_parquet/version"

Gem::Specification.new do |spec|
  spec.name          = "simple_parquet-rb"
  spec.version       = SimpleParquet::VERSION
  spec.authors       = ["Jeremiah Hemphill", "Kyle Finnell"]
  spec.email         = ["jeremiah.hemphill@gmail.com", "tmfinnell@gmail.com"]

  spec.summary       = %q{Read and write a subset of parquet files in ruby}
  spec.description   = %q{Read and write a subset of parquet files in ruby}
  spec.homepage      = "http://www.github.com/jeremiahishere/simple_parquet-rb"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org. To allow pushes either set the 'allowed_push_host'
  # to allow pushing to a single host or delete this section to allow pushing to any host.
  if spec.respond_to?(:metadata)
    spec.metadata["allowed_push_host"] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against " \
      "public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency 'thrift', '~> 0.11.0'
  spec.add_development_dependency "bundler", "~> 1.16"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "pry"
end
