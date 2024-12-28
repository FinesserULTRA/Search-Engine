import { useState } from 'react'
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import { Label } from "@/components/ui/label"
import { cn } from "@/lib/utils"

function DataForm() {
    const [activeTab, setActiveTab] = useState('reviews')
    const [isFileUpload, setIsFileUpload] = useState(false)
    const [hotelFormData, setHotelFormData] = useState({
        name: '',
        region_id: '',
        region: '',
        street_address: '',
        locality: '',
        hotel_class: '',
        service: '',
        cleanliness: '',
        overall: '',
        value: '',
        location: '',
        sleep_quality: '',
        rooms: '',
        average_score: ''
    })

    const [reviewFormData, setReviewFormData] = useState({
        title: '',
        text: '',
        hotel_id: '',
        service: '',
        cleanliness: '',
        overall: '',
        value: '',
        location: '',
        sleep_quality: '',
        rooms: ''
    })

    const handleHotelInputChange = (e) => {
        const { name, value } = e.target
        setHotelFormData(prev => ({ ...prev, [name]: value }))
    }

    const handleReviewInputChange = (e) => {
        const { name, value } = e.target
        setReviewFormData(prev => ({ ...prev, [name]: value }))
    }

    const handleSubmit = (e) => {
        e.preventDefault()
        if (activeTab === 'reviews') {

            if (isFileUpload) {
                console.log('File uploaded')
            } else {

                fetch(`http://127.0.0.1:8000/reviews`, {
                    method: 'POST', // Specify the method
                    headers: {
                        'Content-Type': 'application/json', // Specify content type as JSON
                    },
                    body: JSON.stringify(reviewFormData),
                })
                    .then((res) => res.json())
                    .then((data) => {
                        console.log('Review created:', data);
                    })
                    .catch((err) => {
                        console.error('Error creating review:', err);
                    });

            }

        } else {
            if (isFileUpload) {
                console.log('File uploaded')
            } else {

                fetch(`http://127.0.0.1:8000/hotels`, {
                    method: 'POST', // Specify the method
                    headers: {
                        'Content-Type': 'application/json', // Specify content type as JSON
                    },
                    body: JSON.stringify(hotelFormData),
                })
                    .then((res) => res.json())
                    .then((data) => {
                        console.log('Hotel created:', data);
                    })
                    .catch((err) => {
                        console.error('Error creating hotel:', err);
                    });
            }
        }

        // if (isFileUpload) {
        //     console.log('File uploaded')
        // } else {
        //     console.log('Form submitted:', hotelFormData)
        // }
    }

    return (
        <div className="w-full max-w-2xl mx-auto p-6">
            {/* Custom Tab Switcher */}
            <div className="relative w-full max-w-[300px] h-12 mx-auto mb-8 rounded-full bg-gray-100 p-1.5">
                <div
                    className={cn(
                        "absolute top-1.5 w-[calc(50%-9px)] h-9 rounded-full transition-transform duration-300 bg-white shadow-md",
                        activeTab === 'reviews' ? 'translate-x-[calc(100%+6px)]' : 'translate-x-0'
                    )}
                />
                <div className="relative flex h-full">
                    <button
                        type="button"
                        onClick={() => setActiveTab('hotels')}
                        className={cn(
                            "flex-1 rounded-full text-sm font-medium transition-colors z-10 hover:outline-none hover:border-none focus:border-none focus:outline-none",
                            activeTab === 'hotels' ? 'text-black' : 'text-gray-400'
                        )}
                    >
                        Hotels
                    </button>
                    <button
                        type="button"
                        onClick={() => setActiveTab('reviews')}
                        className={cn(
                            "flex-1 rounded-full text-sm font-medium transition-colors z-10 hover:outline-none hover:border-none focus:outline-none",
                            activeTab === 'reviews' ? 'text-black' : 'text-gray-400'
                        )}
                    >
                        Reviews
                    </button>
                </div>
            </div>
            {activeTab === 'hotels' ? (
                <form onSubmit={handleSubmit} className="space-y-4 bg-[#f0f0f0] rounded-lg p-6">
                    {/* Toggle switch for file upload */}
                    <div className="flex items-center justify-end space-x-2">
                        <Label htmlFor="file-upload-toggle" className="text-sm">File Upload</Label>
                        <Switch
                            id="file-upload-toggle"
                            checked={isFileUpload}
                            onCheckedChange={setIsFileUpload}
                        />
                    </div>

                    {isFileUpload ? (
                        // File Upload Input
                        <div className="space-y-2">
                            <Input
                                type="file"
                                accept=".csv,.json"
                                className="bg-white border-0 cursor-pointer"
                            />
                        </div>
                    ) : (
                        // Manual Entry Form
                        <>
                            {/* First Row */}
                            <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Name</div>
                                    <Input
                                        name="name"
                                        value={hotelFormData.name}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Region ID</div>
                                    <Input
                                        name="region_id"
                                        value={hotelFormData.region_id}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                            </div>

                            {/* Third Row */}
                            <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Region</div>
                                    <Input
                                        name="region"
                                        value={hotelFormData.region}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Street Address</div>
                                    <Input
                                        name="street_address"
                                        value={hotelFormData.street_address}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                            </div>

                            {/* Forth Row */}
                            <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Locality</div>
                                    <Input
                                        name="locality"
                                        value={hotelFormData.locality}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Hotel Class</div>
                                    <Input
                                        name="hotel_class"
                                        value={hotelFormData.hotel_class}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                            </div>

                            {/* Ratings First Row */}
                            <div className="grid grid-cols-4 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Service:</div>
                                    <Input
                                        name="service"
                                        type="number"
                                        value={hotelFormData.service}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Cleanliness:</div>
                                    <Input
                                        name="cleanliness"
                                        type="number"
                                        value={hotelFormData.cleanliness}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Overall:</div>
                                    <Input
                                        name="overall"
                                        type="number"
                                        value={hotelFormData.overall}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Value:</div>
                                    <Input
                                        name="value"
                                        type="number"
                                        value={hotelFormData.value}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                            </div>

                            {/* Ratings Second Row */}
                            <div className="grid grid-cols-4 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Location:</div>
                                    <Input
                                        name="location"
                                        type="number"
                                        value={hotelFormData.location}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Sleep Quality:</div>
                                    <Input
                                        name="sleep_quality"
                                        type="number"
                                        value={hotelFormData.sleep_quality}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Rooms:</div>
                                    <Input
                                        name="rooms"
                                        type="number"
                                        value={hotelFormData.rooms}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Average Score:</div>
                                    <Input
                                        name="average_score"
                                        type="number"
                                        value={hotelFormData.average_score}
                                        onChange={handleHotelInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                            </div>
                        </>
                    )}

                    {/* Submit Button */}
                    <button
                        type="submit"
                        className="w-full bg-zinc-800 text-white py-2 rounded hover:bg-zinc-700 transition-colors"
                    >
                        {isFileUpload ? 'Upload File' : 'Add Hotel'}
                    </button>
                </form>
            ) : (
                <form onSubmit={handleSubmit} className="space-y-4 bg-[#f0f0f0] rounded-lg p-6">
                    {/* Toggle switch for file upload */}
                    <div className="flex items-center justify-end space-x-2">
                        <Label htmlFor="file-upload-toggle" className="text-sm">File Upload</Label>
                        <Switch
                            id="file-upload-toggle"
                            checked={isFileUpload}
                            onCheckedChange={setIsFileUpload}
                        />
                    </div>

                    {isFileUpload ? (
                        // File Upload Input
                        <div className="space-y-2">
                            <Input
                                type="file"
                                accept=".csv,.json"
                                className="bg-white border-0 cursor-pointer"
                            />
                        </div>
                    ) : (
                        // Manual Entry Form
                        <>
                            {/* First Row */}
                            <div className="grid grid-cols-2 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Hotel ID</div>
                                    <Input
                                        name="hotel_id"
                                        value={reviewFormData.hotel_id}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Title</div>
                                    <Input
                                        name="title"
                                        value={reviewFormData.title}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                            </div>

                            {/* Second Row */}
                            <div className="grid grid-cols-1 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Text</div>
                                    <Input
                                        name="text"
                                        type="text"
                                        value={reviewFormData.text}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                    />
                                </div>
                            </div>

                            {/* Ratings First Row */}
                            <div className="grid grid-cols-4 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Overall:</div>
                                    <Input
                                        name="overall"
                                        type="number"
                                        value={reviewFormData.overall}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Value:</div>
                                    <Input
                                        name="value"
                                        type="number"
                                        value={reviewFormData.value}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Location:</div>
                                    <Input
                                        name="location"
                                        type="number"
                                        value={reviewFormData.location}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Cleanliness:</div>
                                    <Input
                                        name="cleanliness"
                                        type="number"
                                        value={reviewFormData.cleanliness}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                            </div>

                            {/* Ratings Second Row */}
                            <div className="grid grid-cols-3 gap-4">
                                <div className="space-y-1">
                                    <div className="text-sm">Service:</div>
                                    <Input
                                        name="service"
                                        type="number"
                                        value={reviewFormData.service}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Sleep Quality:</div>
                                    <Input
                                        name="sleep_quality"
                                        type="number"
                                        value={reviewFormData.sleep_quality}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                                <div className="space-y-1">
                                    <div className="text-sm">Rooms:</div>
                                    <Input
                                        name="rooms"
                                        type="number"
                                        value={reviewFormData.rooms}
                                        onChange={handleReviewInputChange}
                                        className="bg-white border-0"
                                        min="0"
                                        max="5"
                                    />
                                </div>
                            </div>
                        </>
                    )}

                    {/* Submit Button */}
                    <button
                        type="submit"
                        className="w-full bg-zinc-800 text-white py-2 rounded hover:bg-zinc-700 transition-colors"
                    >
                        {isFileUpload ? 'Upload File' : 'Add Review'}
                    </button>
                </form>
            )}
        </div>
    )
}

export default DataForm;