/**
 * Copyright (C) 2018 Blue Brain Project
 * All rights reserved. Do not distribute without further notice.
 *
 * @author Fernando Pereira <fernando.pereira@epfl.ch>
 *
 */
#ifndef LIB_TOUCHES_TOUCH_DEFS_H_
#define LIB_TOUCHES_TOUCH_DEFS_H_

namespace neuron_parquet {
namespace touches {

enum TOUCH_LOCATION { NEURON_ID, SECTION_ID, SEGMENT_ID };

struct Touch {
    inline int getPreNeuronID() const {
        return pre_synapse_ids[NEURON_ID];
    }
    inline int getPostNeuronID() const {
        return post_synapse_ids[NEURON_ID];
    }

    int pre_synapse_ids[3];
    int post_synapse_ids[3];
    int branch;
    float distance_soma;
    float pre_offset, post_offset;
};


}  // namespace touches
}  // namespace neuron_parquet

#endif  // LIB_TOUCHES_TOUCH_DEFS_H_
