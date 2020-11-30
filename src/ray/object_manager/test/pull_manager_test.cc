
#include "ray/object_manager/pull_manager.h"

#include "gtest/gtest.h"
#include "ray/common/test_util.h"

namespace ray {

class PullManagerTest : public ::testing::Test {
 public:
  PullManagerTest()
      : self_node_id_(NodeID::FromRandom()),
        object_is_local_(false),
        num_send_pull_request_calls_(0),
        rand_int_(0),
        num_restore_spilled_object_calls_(0),
        pull_manager_(self_node_id_,
                      [this](const ObjectID &object_id) { return object_is_local_; },
                      [this](const ObjectID &object_id, const NodeID &node_id) {
                        num_send_pull_request_calls_++;
                      },
                      [this](int) { return rand_int_; },
                      [this](const ObjectID &, const std::string &,
                             std::function<void(const ray::Status &)>) {
                        num_restore_spilled_object_calls_++;
                      }) {}

  NodeID self_node_id_;
  bool object_is_local_;
  int num_send_pull_request_calls_;
  int rand_int_;
  int num_restore_spilled_object_calls_;
  PullManager pull_manager_;
};

TEST_F(PullManagerTest, TestStaleSubscription) {
  ObjectID obj1 = ObjectID::FromRandom();
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  pull_manager_.Pull(obj1, addr1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  const std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(ObjectID::FromRandom(), client_ids, "");

  // client_ids is empty here, so there's no node to send a request to.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  pull_manager_.CancelPull(obj1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

  TEST_F(PullManagerTest, TestManyUpdates) {
    ObjectID obj1 = ObjectID::FromRandom();
    rpc::Address addr1;
    ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
    pull_manager_.Pull(obj1, addr1);
    ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

    std::unordered_set<NodeID> client_ids;
    client_ids.insert(NodeID::FromRandom());

    for (int i = 0; i < 100; i++) {
      pull_manager_.OnLocationChange(obj1, client_ids, "");
    }

    ASSERT_EQ(num_send_pull_request_calls_, 100);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);

    pull_manager_.CancelPull(obj1);
    ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  }


TEST_F(PullManagerTest, TestBasic) {
  ObjectID obj1 = ObjectID::FromRandom();
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  pull_manager_.Pull(obj1, addr1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "");

  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  pull_manager_.CancelPull(obj1);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
