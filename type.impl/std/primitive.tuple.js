(function() {
    var type_impls = Object.fromEntries([["arrow",[]],["arrow_cast",[]],["arrow_select",[]]]);
    if (window.register_type_impls) {
        window.register_type_impls(type_impls);
    } else {
        window.pending_type_impls = type_impls;
    }
})()
//{"start":55,"fragment_lengths":[12,18,20]}