
#include "ray/object_manager/pull_manager.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/common_protocol.h"
#include "ray/common/test_util.h"

namespace ray {

using ::testing::ElementsAre;

class PullManagerTest : public ::testing::Test {
 public:
  PullManagerTest()
      : self_node_id_(NodeID::FromRandom()),
        object_is_local_(false),
        num_send_pull_request_calls_(0),
        num_restore_spilled_object_calls_(0),
        fake_time_(0),
        pull_manager_(self_node_id_,
                      [this](const ObjectID &object_id) { return object_is_local_; },
                      [this](const ObjectID &object_id, const NodeID &node_id) {
                        num_send_pull_request_calls_++;
                        last_pulled_node_ = node_id;
                      },
                      [this](const ObjectID &, const std::string &,
                             std::function<void(const ray::Status &)>) {
                        num_restore_spilled_object_calls_++;
                      },
                      [this]() { return fake_time_; }, 10000) {}

  NodeID self_node_id_;
  bool object_is_local_;
  int num_send_pull_request_calls_;
  int num_restore_spilled_object_calls_;
  double fake_time_;
  PullManager pull_manager_;
  NodeID last_pulled_node_;
};

std::vector<rpc::ObjectReference> CreateObjectRefs(int num_objs) {
  std::vector<rpc::ObjectReference> refs;
  for (int i = 0; i < num_objs; i++) {
    ObjectID obj = ObjectID::FromRandom();
    rpc::ObjectReference ref;
    ref.set_object_id(obj.Binary());
    refs.push_back(ref);
  }
  return refs;
}

TEST_F(PullManagerTest, TestStaleSubscription) {
  auto refs = CreateObjectRefs(1);
  auto oid = ObjectRefsToIds(refs)[0];
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(oid, client_ids, "");

  // There are no client ids to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));

  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);

  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(oid, client_ids, "");

  // Now we're getting a notification about an object that was already cancelled.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestRestoreSpilledObject) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar");

  // client_ids is empty here, so there's nowhere to pull from.
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 1);

  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar");

  // The behavior is supposed to be to always restore the spilled object if possible (even
  // if it exists elsewhere in the cluster).
  ASSERT_EQ(num_send_pull_request_calls_, 0);
  ASSERT_EQ(num_restore_spilled_object_calls_, 2);

  // Don't restore an object if it's local.
  object_is_local_ = true;
  num_restore_spilled_object_calls_ = 0;
  pull_manager_.OnLocationChange(obj1, client_ids, "remote_url/foo/bar");
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestManyUpdates) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  pull_manager_.OnLocationChange(obj1, client_ids, "");
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  for (int i = 0; i < 100; i++) {
    pull_manager_.OnLocationChange(obj1, client_ids, "");
  }

  // There is already an in progress pull request, so there is no reason to send another
  // request before the retry time.
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestRetryTimer) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());

  // We need to call OnLocationChange at least once, to population the list of nodes with
  // the object.
  pull_manager_.OnLocationChange(obj1, client_ids, "");
  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  for (; fake_time_ <= 127 * 10; fake_time_ += 1.) {
    pull_manager_.Tick();
    // This OnLocationChange shouldn't trigger an additional pull request or update the
    // retry time.
    pull_manager_.OnLocationChange(obj1, client_ids, "");
  }

  ASSERT_EQ(num_send_pull_request_calls_, 1 + 7);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // Don't retry an object if it's local.
  object_is_local_ = true;
  num_send_pull_request_calls_ = 0;
  for (; fake_time_ <= 127 * 10; fake_time_ += 1.) {
    pull_manager_.Tick();
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, ObjectRefsToIds(refs));
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
}

TEST_F(PullManagerTest, TestDedupe) {
  auto refs = CreateObjectRefs(1);
  auto obj1 = ObjectRefsToIds(refs)[0];
  rpc::Address addr1;
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 1);

  std::unordered_set<NodeID> pulled_nodes;

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "");
  pulled_nodes.insert(last_pulled_node_);

  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // 2 additional nodes have the object. We don't pull it yet though, because the retry
  // timer hasn't gone off yet.
  client_ids.insert(NodeID::FromRandom());
  client_ids.insert(NodeID::FromRandom());
  pull_manager_.OnLocationChange(obj1, client_ids, "");

  ASSERT_EQ(num_send_pull_request_calls_, 1);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  fake_time_ += 10;
  pull_manager_.Tick();
  pulled_nodes.insert(last_pulled_node_);

  fake_time_ += 20;
  pull_manager_.Tick();
  pulled_nodes.insert(last_pulled_node_);

  // Now that 2 retry intervals have passed, we should've pulled from both of the other
  // nodes.
  ASSERT_EQ(num_send_pull_request_calls_, 3);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_EQ(pulled_nodes.size(), 3);

  fake_time_ += 40;
  pull_manager_.Tick();
  pulled_nodes.insert(last_pulled_node_);

  // Since we've sent a pull to every node we know about, we should now retry from a node
  // we've already pulled from.
  ASSERT_EQ(num_send_pull_request_calls_, 4);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  ASSERT_EQ(pulled_nodes.size(), 3);
}

TEST_F(PullManagerTest, TestBasic) {
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), oids.size());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "");
    ASSERT_EQ(num_send_pull_request_calls_, i + 1);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  }

  // Don't pull an object if it's local.
  object_is_local_ = true;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "");
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);

  auto objects_to_cancel = pull_manager_.CancelPull(req_id);
  ASSERT_EQ(objects_to_cancel, oids);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);

  // Don't pull a remote object if we've canceled.
  object_is_local_ = false;
  num_send_pull_request_calls_ = 0;
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "");
  }
  ASSERT_EQ(num_send_pull_request_calls_, 0);
}

TEST_F(PullManagerTest, TestDeduplicateBundles) {
  auto refs = CreateObjectRefs(3);
  auto oids = ObjectRefsToIds(refs);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto req_id1 = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_EQ(ObjectRefsToIds(objects_to_locate), oids);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), oids.size());

  objects_to_locate.clear();
  auto req_id2 = pull_manager_.Pull(refs, &objects_to_locate);
  ASSERT_TRUE(objects_to_locate.empty());

  std::unordered_set<NodeID> client_ids;
  client_ids.insert(NodeID::FromRandom());
  for (size_t i = 0; i < oids.size(); i++) {
    pull_manager_.OnLocationChange(oids[i], client_ids, "");
    ASSERT_EQ(num_send_pull_request_calls_, i + 1);
    ASSERT_EQ(num_restore_spilled_object_calls_, 0);
  }

  // Cancel one request.
  auto objects_to_cancel = pull_manager_.CancelPull(req_id1);
  ASSERT_TRUE(objects_to_cancel.empty());
  // Objects should still be pulled because the other request is still open.
  ASSERT_EQ(pull_manager_.NumActiveRequests(), oids.size());
  num_send_pull_request_calls_ = 0;
  fake_time_ += 1024 * 10;
  pull_manager_.Tick();
  ASSERT_EQ(num_send_pull_request_calls_, 3);
  ASSERT_EQ(num_restore_spilled_object_calls_, 0);

  // Cancel the other request.
  objects_to_cancel = pull_manager_.CancelPull(req_id2);
  ASSERT_EQ(objects_to_cancel, oids);
  ASSERT_EQ(pull_manager_.NumActiveRequests(), 0);

  // Don't pull a remote object if we've canceled.
  object_is_local_ = false;
  num_send_pull_request_calls_ = 0;
  fake_time_ += 1024 * 10 + 1;
  pull_manager_.Tick();

  ASSERT_EQ(num_send_pull_request_calls_, 0);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
