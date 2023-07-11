/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/node/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NodePoolSpec defines the desired state of NodePool
type NodePoolSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// Taints 污点
	Taints []corev1.Taint `json:"taints,omitempty"`

	// Labels 标签
	Labels map[string]string `json:"labels,omitempty"`

	// 新增加字段
	Foo string `json:"foo,omitempty"`

	// Handler 对应 Runtime Class 的 Handler
	Handler string `json:"handler,omitempty"`

	// 新增字段2
	Foo2 string `json:"foo2,omitempty"`
}

// NodePoolStatus defines the observed state of NodePool
type NodePoolStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	Status int `json:"status"`

	// 节点的数量
	NodeCount int `json:"nodeCount"`

	// 允许被调度的容量
	Allocatable corev1.ResourceList `json:"allocatable,omitempty" protobuf:"bytes,2,rep,name=allocatable,casttype=ResourceList,castkey=ResourceName"`
}

//+kubebuilder:printcolumn:JSONPath=".status.status",name=Status,type=integer
//+kubebuilder:printcolumn:JSONPath=".status.nodeCount",name=NodeCount,type=integer
//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:scope=Cluster

// NodePool is the Schema for the nodepools API
type NodePool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NodePoolSpec   `json:"spec,omitempty"`
	Status NodePoolStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// NodePoolList contains a list of NodePool
type NodePoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodePool `json:"items"`
}

func (n *NodePool) NodeLabelSelector() labels.Selector {
	return labels.SelectorFromSet(map[string]string{
		n.NodeRole(): "",
	})
}

func (n *NodePool) NodeRole() string {
	return "node-role.kubernetes.io/" + n.Name
}

var (
	nodePoolLog = logf.Log.WithName("nodepool-resource")
	keyReg      = regexp.MustCompile(`^node-pool.lailin.xyz/*[a-zA-z0-9]*$`)
)

// 根据nodepool标签，然后给节点打标签
func (s *NodePoolSpec) ApplyNode(node corev1.Node) *corev1.Node {
	// 如果节点上存在不属于当前节点池的标签，我们就清除掉
	// 注意：这里的逻辑如果一个节点属于多个节点池会出现问题
	nodeLabels := make(map[string]string)
	for k, v := range nodeLabels {
		if !keyReg.MatchString(k) {
			nodeLabels[k] = v
		}
	}
	for k, v := range s.Labels {
		nodeLabels[k] = v
	}
	node.Labels = nodeLabels
	// 污点同理
	var taints []corev1.Taint
	for _, taint := range node.Spec.Taints {
		if !keyReg.MatchString(taint.Key) {
			taints = append(taints, taint)
		}
	}

	node.Spec.Taints = append(taints, s.Taints...)
	return &node
}

// CleanNode 清理节点标签污点信息，仅保留系统标签
func (s *NodePoolSpec) CleanNode(node corev1.Node) *corev1.Node {
	// 除了节点池的标签之外，我们只保留 k8s 的相关标签
	// 注意：这里的逻辑如果一个节点只能属于一个节点池
	nodeLabels := map[string]string{}
	for k, v := range node.Labels {
		if strings.Contains(k, "kubernetes") {
			nodeLabels[k] = v
		}
	}
	node.Labels = nodeLabels

	// 污点同理
	var taints []corev1.Taint
	for _, taint := range node.Spec.Taints {
		if strings.Contains(taint.Key, "kubernetes") {
			taints = append(taints, taint)
		}
	}
	node.Spec.Taints = taints
	return &node
}

// 创建runtimeClass对象
func (n *NodePool) RuntimeClass() *v1beta1.RuntimeClass {
	s := n.Spec
	tolerations := make([]corev1.Toleration, len(s.Taints))
	for i, t := range s.Taints {
		tolerations[i] = corev1.Toleration{
			Key:      t.Key,
			Value:    t.Value,
			Effect:   t.Effect,
			Operator: corev1.TolerationOpEqual,
		}
	}

	return &v1beta1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-pool-" + n.Name,
		},
		Handler: "runc",
		Scheduling: &v1beta1.Scheduling{
			NodeSelector: s.Labels,
			Tolerations:  tolerations,
		},
	}
}

func init() {
	SchemeBuilder.Register(&NodePool{}, &NodePoolList{})
}
