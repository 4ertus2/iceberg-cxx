#include <YdbModes/SsaProgram.h>
#include <YdbModes/switch_type.h>
#include <arrow/visit_data_inline.h>

namespace iceberg::udf {

template <template <typename U> class Generator, typename T>
arrow::compute::ArrayKernelExec GenerateVarBinaryToVarBinary(const T& type) {
  using Type = arrow::Type;

  switch (type->id()) {
    case Type::BINARY:
      return Generator<arrow::BinaryType>::Exec;
    case Type::STRING:
      return Generator<arrow::StringType>::Exec;
    case Type::LARGE_BINARY:
      return Generator<arrow::LargeBinaryType>::Exec;
    case Type::LARGE_STRING:
      return Generator<arrow::LargeStringType>::Exec;
    default:
      return nullptr;
  }
}

struct ReplaceKernelBase {
  struct Options /*: public arrow::compute::FunctionOptions*/ {
    static std::unordered_map<std::string, std::string> replaces;
  };

  static arrow::Status Replace(std::string_view str, arrow::TypedBufferBuilder<uint8_t>& builder) {
    if (str.empty()) {
      return arrow::Status::OK();
    }

    const auto& replaces_map = Options::replaces;
    auto it = replaces_map.find(std::string(str));
    if (it != replaces_map.end()) {
      return Append(it->second, builder);
    }
    return Append(str, builder);
  }

  static arrow::Status Append(std::string_view str, arrow::TypedBufferBuilder<uint8_t>& builder) {
    return builder.Append(reinterpret_cast<const uint8_t*>(str.data()), static_cast<int64_t>(str.length()));
  }

 private:
  ~ReplaceKernelBase() = default;
};

std::unordered_map<std::string, std::string> ReplaceKernelBase::Options::replaces;

template <typename Type>
struct ReplaceKernel : private ReplaceKernelBase {
  using offset_type = typename Type::offset_type;
  using ValueDataBuilder = arrow::TypedBufferBuilder<uint8_t>;
  using OffsetBuilder = arrow::TypedBufferBuilder<offset_type>;

  static arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecSpan& batch,
                            arrow::compute::ExecResult* out) {
    // auto& options = State::Get(ctx);
    ValueDataBuilder value_data_builder(ctx->memory_pool());
    OffsetBuilder offset_builder(ctx->memory_pool());

    // We already know how many strings we have, so we can use Reserve/UnsafeAppend
    RETURN_NOT_OK(offset_builder.Reserve(batch.length + 1));
    offset_builder.UnsafeAppend(0);  // offsets start at 0

    const arrow::compute::ExecValue& value = batch[0];  // first argument
    RETURN_NOT_OK(arrow::VisitArraySpanInline<Type>(
        value.array,
        [&](std::string_view s) {
          RETURN_NOT_OK(Replace(s, value_data_builder));
          offset_type offset = value_data_builder.length();
          offset_builder.UnsafeAppend(offset);
          return arrow::Status::OK();
        },
        [&]() {
          // offset for null value
          offset_type offset = value_data_builder.length();
          offset_builder.UnsafeAppend(offset);
          return arrow::Status::OK();
        }));

    arrow::ArrayData* output = out->array_data_mutable();
    RETURN_NOT_OK(value_data_builder.Finish(&output->buffers[2]));
    return offset_builder.Finish(&output->buffers[1]);
  }
};

void RegisterReplace(const std::string& func_name, const std::unordered_map<std::string, std::string>& replaces) {
  using ScalarFunction = arrow::compute::ScalarFunction;
  using ScalarKernel = arrow::compute::ScalarKernel;
  using FunctionDoc = arrow::compute::FunctionDoc;
  using Arity = arrow::compute::Arity;

  auto ctx = arrow::compute::default_exec_context();
  auto registry = ctx->func_registry();

  if (!registry) {
    throw std::runtime_error(__FUNCTION__);
  }
  if (registry->GetFunction(func_name).ok()) {
    return;
  }

  // TODO(a.v.zuykov): use FunctionOptions instead?
  ReplaceKernelBase::Options::replaces = replaces;

  auto func = std::make_shared<ScalarFunction>(func_name, Arity::Unary(), FunctionDoc::Empty());
  for (const auto& type : arrow::BaseBinaryTypes()) {
    auto exec = GenerateVarBinaryToVarBinary<ReplaceKernel>(type);
    ScalarKernel kernel{{type}, type, std::move(exec)};
    kernel.mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    AHY::ensure(func->AddKernel(std::move(kernel)));
  }
  AHY::ensure(registry->AddFunction(std::move(func)));
}

}  // namespace iceberg::udf
