// vim: ts=8 sw=2 smarttab

#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/log_message.h"

#define dout_context g_ceph_context

int main(int argc, const char *argv[])
{
	vector<const char*> args;
	argv_to_vec(argc, argv, args);

	auto cct = global_init(NULL, args,
			CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
			CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

	common_init_finish(g_ceph_context);
	g_ceph_context->_conf.apply_changes(nullptr);

	MonClient monclient(g_ceph_context);

	int err = monclient.build_initial_monmap();
	if (err < 0) {
		return err;
	}

	Messenger* messenger = Messenger::create_client_messenger(g_ceph_context, "radosclient");
	messenger->set_default_policy(Messenger::Policy::lossy_client(CEPH_FEATURE_OSDREPLYMUX));
	monclient.set_messenger(messenger);

	monclient.set_want_keys(
			CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD | CEPH_ENTITY_TYPE_MGR);

	err = monclient.init();
    if (err < 0) {
		return err;
	}

	err = monclient.authenticate(cct->_conf->client_mount_timeout);
    if (err < 0) {
		monclient.shutdown();
		return err;
	}


	std::vector<std::string> cmd{"{\"prefix\": \"status\", \"format\": \"json-pretty\"}"};
	bufferlist inbl;
	bufferlist outbl;
	string outs;

	C_SaferCond ctx;
	monclient.start_mon_command(cmd, inbl, &outbl, &outs, &ctx);
	int result = ctx.wait();
	LOG(INFO) << "result " << result << " outs " << outs << " outbl " << outbl.c_str();

	monclient.shutdown();
	return 0;
}
